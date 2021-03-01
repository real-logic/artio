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
import uk.co.real_logic.artio.fixp.AbstractFixPOffsets;
import uk.co.real_logic.artio.fixp.AbstractFixPParser;
import uk.co.real_logic.artio.fixp.FixPProtocol;
import uk.co.real_logic.artio.fixp.FixPProtocolFactory;
import uk.co.real_logic.artio.messages.FixPMessageDecoder;
import uk.co.real_logic.artio.messages.FixPProtocolType;
import uk.co.real_logic.artio.messages.ILinkConnectDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_FLAG;
import static uk.co.real_logic.artio.engine.SessionInfo.UNK_SESSION;
import static uk.co.real_logic.artio.fixp.AbstractFixPParser.BOOLEAN_FLAG_TRUE;
import static uk.co.real_logic.artio.fixp.AbstractFixPParser.ILINK_MESSAGE_HEADER_LENGTH;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.SOFH_LENGTH;

class FixPSequenceNumberExtractor
{
    private final Long2LongHashMap connectionIdToILinkUuid;
    private final ErrorHandler errorHandler;
    private final FixPProtocolType fixPProtocolType;
    private final ILinkSequenceNumberHandler handler;

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixPMessageDecoder iLinkMessage = new FixPMessageDecoder();
    private final ILinkConnectDecoder iLinkConnect = new ILinkConnectDecoder();

    private AbstractFixPOffsets offsets;
    private AbstractFixPParser parser;
    private boolean attemptedILinkInit = false;

    FixPSequenceNumberExtractor(
        final Long2LongHashMap connectionIdToILinkUuid,
        final ErrorHandler errorHandler,
        final FixPProtocolType fixPProtocolType,
        final ILinkSequenceNumberHandler handler)
    {
        this.connectionIdToILinkUuid = connectionIdToILinkUuid;
        this.errorHandler = errorHandler;
        this.fixPProtocolType = fixPProtocolType;
        this.handler = handler;
    }

    public void onFragment(
        final DirectBuffer buffer,
        final int srcOffset,
        final int srcLength,
        final Header header)
    {
        final long endPosition = header.position();

        if ((header.flags() & BEGIN_FLAG) == BEGIN_FLAG)
        {
            int offset = srcOffset;
            messageHeader.wrap(buffer, offset);

            offset += messageHeader.encodedLength();
            final int actingBlockLength = messageHeader.blockLength();
            final int version = messageHeader.version();
            final int templateId = messageHeader.templateId();

            switch (templateId)
            {
                case FixPMessageDecoder.TEMPLATE_ID:
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

            final FixPProtocol protocol = FixPProtocolFactory.make(fixPProtocolType, errorHandler);
            if (protocol == null)
            {
                errorHandler.onError(new IllegalStateException(
                    "Configuration Issue: could not setup Binary FIXP protocol on the Engine classpath, despite " +
                    "Binary FIXP message requiring processing. Sequence Index update ignored. " +
                    "If you're using iLink3 then you should be the artio-ilink3-codecs and artio-ilink3-impl" +
                    "dependencies on the classpath. " +
                    "Binary entrypoint requires a call to EngineConfiguration.acceptBinaryEntryPoint()"));
                return;
            }

            parser = protocol.makeParser(null);
            offsets = protocol.makeOffsets();
        }

        iLinkMessage.wrap(buffer, offset, actingBlockLength, version);
        final long connectionId = iLinkMessage.connection();

        final int sofhOffset = offset + FixPMessageDecoder.BLOCK_LENGTH;
        final int headerOffset = sofhOffset + SOFH_LENGTH;
        final int templateId = parser.templateId(buffer, headerOffset);
        final int messageOffset = headerOffset + ILINK_MESSAGE_HEADER_LENGTH;
        final boolean possRetrans = offsets.possRetrans(templateId, buffer, messageOffset) == BOOLEAN_FLAG_TRUE;

        final int seqNum = offsets.seqNum(templateId, buffer, messageOffset);
        if (seqNum != AbstractFixPOffsets.MISSING_OFFSET)
        {
            final long uuid = connectionIdToILinkUuid.get(connectionId);
            if (uuid != UNK_SESSION)
            {
                handler.onSequenceNumber(seqNum, uuid, totalLength, endPosition, aeronSessionId, possRetrans);
            }
        }
    }
}
