/*
 * Copyright 2015-2019 Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.engine.logger;

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.ilink.ILinkMessageConsumer;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.ILinkMessageDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;

import static uk.co.real_logic.artio.messages.FixMessageDecoder.metaDataHeaderLength;
import static uk.co.real_logic.artio.messages.FixMessageDecoder.metaDataSinceVersion;

class LogEntryHandler implements ControlledFragmentHandler
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final ILinkMessageDecoder iLinkMessage = new ILinkMessageDecoder();

    private final FixMessageConsumer fixHandler;
    private final ILinkMessageConsumer iLinkHandler;

    LogEntryHandler(final FixMessageConsumer fixHandler, final ILinkMessageConsumer iLinkHandler)
    {
        this.fixHandler = fixHandler;
        this.iLinkHandler = iLinkHandler;
    }

    @SuppressWarnings("FinalParameters")
    public Action onFragment(
        final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        messageHeader.wrap(buffer, offset);
        final int templateId = messageHeader.templateId();
        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();

        if (templateId == FixMessageDecoder.TEMPLATE_ID)
        {
            offset += MessageHeaderDecoder.ENCODED_LENGTH;

            fixMessage.wrap(buffer, offset, blockLength, version);

            if (version >= metaDataSinceVersion())
            {
                offset += metaDataHeaderLength() + fixMessage.metaDataLength();
                fixMessage.skipMetaData();
            }

            fixHandler.onMessage(fixMessage, buffer, offset, length, header);
        }
        else if (templateId == ILinkMessageDecoder.TEMPLATE_ID)
        {
            offset += MessageHeaderDecoder.ENCODED_LENGTH;

            iLinkMessage.wrap(buffer, offset, blockLength, version);

            offset += ILinkMessageDecoder.BLOCK_LENGTH;

            iLinkHandler.onBusinessMessage(iLinkMessage, buffer, offset, header);
        }

        return Action.CONTINUE;
    }
}
