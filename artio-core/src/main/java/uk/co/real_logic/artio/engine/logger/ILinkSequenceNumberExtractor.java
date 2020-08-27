/*
 * Copyright 2020 Monotonic Ltd.
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

import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.Long2LongHashMap;
import uk.co.real_logic.artio.ilink.AbstractILink3Offsets;
import uk.co.real_logic.artio.ilink.AbstractILink3Parser;
import uk.co.real_logic.artio.messages.ILinkConnectDecoder;
import uk.co.real_logic.artio.messages.ILinkMessageDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_AND_END_FLAGS;
import static uk.co.real_logic.artio.engine.SessionInfo.UNK_SESSION;
import static uk.co.real_logic.artio.ilink.AbstractILink3Parser.BOOLEAN_FLAG_TRUE;
import static uk.co.real_logic.artio.ilink.AbstractILink3Parser.ILINK_MESSAGE_HEADER_LENGTH;
import static uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader.SOFH_LENGTH;

class ILinkSequenceNumberExtractor
{
    private final Long2LongHashMap connectionIdToILinkUuid;
    private final ErrorHandler errorHandler;
    private final ILinkSequenceNumberHandler handler;

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final ILinkMessageDecoder iLinkMessage = new ILinkMessageDecoder();
    private final ILinkConnectDecoder iLinkConnect = new ILinkConnectDecoder();

    private AbstractILink3Offsets offsets;
    private AbstractILink3Parser parser;
    private boolean attemptedILinkInit = false;

    ILinkSequenceNumberExtractor(
        final Long2LongHashMap connectionIdToILinkUuid,
        final ErrorHandler errorHandler,
        final ILinkSequenceNumberHandler handler)
    {
        this.connectionIdToILinkUuid = connectionIdToILinkUuid;
        this.errorHandler = errorHandler;
        this.handler = handler;
    }

    public void onFragment(
        final DirectBuffer buffer,
        final int srcOffset,
        final int srcLength,
        final Header header)
    {
        final long endPosition = header.position();

        if ((header.flags() & BEGIN_AND_END_FLAGS) == BEGIN_AND_END_FLAGS)
        {
            int offset = srcOffset;
            messageHeader.wrap(buffer, offset);

            offset += messageHeader.encodedLength();
            final int actingBlockLength = messageHeader.blockLength();
            final int version = messageHeader.version();
            final int templateId = messageHeader.templateId();


            switch (templateId)
            {
                case ILinkMessageDecoder.TEMPLATE_ID:
                {
                    final int totalLength = BitUtil.align(srcLength, FRAME_ALIGNMENT);

                    onILinkMessage(
                        buffer, endPosition, offset, actingBlockLength, version, totalLength, header.sessionId());
                    break;
                }

                case ILinkConnectDecoder.TEMPLATE_ID:
                {
                    iLinkConnect.wrap(buffer, offset, actingBlockLength, version);
                    connectionIdToILinkUuid.put(iLinkConnect.connection(), iLinkConnect.uuid());
                    break;
                }
            }
        }
    }

    private void onILinkMessage(
        final DirectBuffer buffer,
        final long endPosition,
        final int offset,
        final int actingBlockLength,
        final int version,
        final int totalLength,
        final int aeronSessionId)
    {
        if (!attemptedILinkInit)
        {
            attemptedILinkInit = true;

            parser = AbstractILink3Parser.make(null, errorHandler);
            offsets = AbstractILink3Offsets.make(errorHandler);

            if (parser == null || offsets == null)
            {
                errorHandler.onError(new IllegalStateException(
                    "Configuration Issue: could not find ILink3Codes on the Engine classpath, despite " +
                    "ILink3 message requiring processing. Sequence Index update ignored"));
                return;
            }
        }

        iLinkMessage.wrap(buffer, offset, actingBlockLength, version);
        final long connectionId = iLinkMessage.connection();

        final int sofhOffset = offset + ILinkMessageDecoder.BLOCK_LENGTH;
        final int headerOffset = sofhOffset + SOFH_LENGTH;
        final int templateId = parser.templateId(buffer, headerOffset);
        final int messageOffset = headerOffset + ILINK_MESSAGE_HEADER_LENGTH;
        final int possRetrans = offsets.possRetrans(templateId, buffer, messageOffset);
        if (possRetrans == BOOLEAN_FLAG_TRUE)
        {
            return;
        }

        final int seqNum = offsets.seqNum(templateId, buffer, messageOffset);
        if (seqNum != AbstractILink3Offsets.MISSING_OFFSET)
        {
            final long uuid = connectionIdToILinkUuid.get(connectionId);
            if (uuid != UNK_SESSION)
            {
                handler.onSequenceNumber(seqNum, uuid, totalLength, endPosition, aeronSessionId);
            }
        }
    }
}
