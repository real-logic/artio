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
package uk.co.real_logic.fix_gateway.streams;

import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.session.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.*;

import static uk.co.real_logic.fix_gateway.messages.MessageStatus.OK;
import static uk.co.real_logic.fix_gateway.streams.GatewayPublication.FRAME_SIZE;
import static uk.co.real_logic.fix_gateway.streams.Streams.UNKNOWN_TEMPLATE;

public class SessionSubscription implements FragmentHandler
{

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final DisconnectDecoder disconnect = new DisconnectDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();

    private final SessionHandler sessionHandler;

    public SessionSubscription(final SessionHandler sessionHandler)
    {
        this.sessionHandler = sessionHandler;
    }

    public void onFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        readFragment(buffer, offset, header);
    }

    public FragmentHandler andThen(final ProcessProtocolSubscription other)
    {
        return (buffer, offset, length, header) ->
        {
            if (readFragment(buffer, offset, header) == UNKNOWN_TEMPLATE)
            {
                other.onFragment(buffer, offset, length, header);
            }
        };
    }

    public int readFragment(final DirectBuffer buffer, int offset, final Header header)
    {
        messageHeader.wrap(buffer, offset);

        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();
        offset += messageHeader.encodedLength();

        switch (messageHeader.templateId())
        {
            case FixMessageDecoder.TEMPLATE_ID:
            {
                return onFixMessage(buffer, offset, blockLength, version);
            }

            case DisconnectDecoder.TEMPLATE_ID:
            {
                return onDisconnect(buffer, offset, blockLength, version);
            }
        }

        return Streams.UNKNOWN_TEMPLATE;
    }

    private int onDisconnect(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        disconnect.wrap(buffer, offset, blockLength, version);
        final long connectionId = disconnect.connection();
        DebugLogger.log("FixSubscription Disconnect: %d\n", connectionId);
        sessionHandler.onDisconnect(disconnect.libraryId(), connectionId, disconnect.reason());
        return offset + DisconnectDecoder.BLOCK_LENGTH;
    }

    private int onFixMessage(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        messageFrame.wrap(buffer, offset, blockLength, version);
        final int messageLength = messageFrame.bodyLength();
        if (messageFrame.status() == OK)
        {
            sessionHandler.onMessage(
                buffer,
                offset + FRAME_SIZE,
                messageLength,
                messageFrame.libraryId(),
                messageFrame.connection(),
                messageFrame.session(),
                messageFrame.messageType(),
                messageFrame.timestamp());
        }

        return offset + FRAME_SIZE + messageLength;
    }
}
