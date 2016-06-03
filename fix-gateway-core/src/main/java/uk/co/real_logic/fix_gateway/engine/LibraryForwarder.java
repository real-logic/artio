/*
 * Copyright 2015-2016 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.fix_gateway.engine;

import io.aeron.Publication;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.LibraryConnectDecoder;
import uk.co.real_logic.fix_gateway.messages.NotLeaderEncoder;
import uk.co.real_logic.sbe.ir.generated.MessageHeaderDecoder;
import uk.co.real_logic.sbe.ir.generated.MessageHeaderEncoder;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

public class LibraryForwarder implements ControlledFragmentHandler
{
    private static final int REPLY_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH + NotLeaderEncoder.BLOCK_LENGTH;

    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(new byte[REPLY_LENGTH]);
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final NotLeaderEncoder notLeaderEncoder = new NotLeaderEncoder();
    private final LibraryConnectDecoder libraryConnectDecoder = new LibraryConnectDecoder();
    private final Publication replyPublication;

    public LibraryForwarder(final Publication replyPublication)
    {
        this.replyPublication = replyPublication;

        messageHeaderEncoder
            .wrap(unsafeBuffer, 0)
            .templateId(NotLeaderEncoder.TEMPLATE_ID);

        notLeaderEncoder
            .wrap(unsafeBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        if (LibraryConnectDecoder.TEMPLATE_ID == messageHeaderDecoder.templateId())
        {
            libraryConnectDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            messageHeaderEncoder
                .schemaId(messageHeaderDecoder.schemaId())
                .version(messageHeaderDecoder.version())
                .blockLength(NotLeaderEncoder.BLOCK_LENGTH);

            notLeaderEncoder
                .correlationId(libraryConnectDecoder.correlationId())
                .libraryId(libraryConnectDecoder.libraryId());

            if (replyPublication.offer(unsafeBuffer) < 0)
            {
                return ABORT;
            }
        }

        return CONTINUE;
    }
}
