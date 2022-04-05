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
import uk.co.real_logic.artio.fixp.AbstractFixPSequenceExtractor;
import uk.co.real_logic.artio.fixp.FixPProtocol;
import uk.co.real_logic.artio.fixp.FixPProtocolFactory;
import uk.co.real_logic.artio.messages.*;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_FLAG;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.SOFH_LENGTH;

class FixPSequenceIndexer
{
    private final Long2LongHashMap connectionIdToFixPSessionId;
    private final ErrorHandler errorHandler;
    private final FixPProtocolType fixPProtocolType;
    private final FixPSequenceNumberHandler handler;
    private final SequenceNumberIndexReader sequenceNumberReader;

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixPMessageDecoder fixPMessage = new FixPMessageDecoder();
    private final ILinkConnectDecoder iLinkConnect = new ILinkConnectDecoder();
    private final InboundFixPConnectDecoder inboundFixPConnect = new InboundFixPConnectDecoder();
    private final FollowerSessionRequestDecoder followerSessionRequest = new FollowerSessionRequestDecoder();

    private AbstractFixPSequenceExtractor sequenceExtractor;
    private boolean attemptedProtocolInit = false;

    FixPSequenceIndexer(
        final Long2LongHashMap connectionIdToFixPSessionId,
        final ErrorHandler errorHandler,
        final FixPProtocolType fixPProtocolType,
        final SequenceNumberIndexReader sequenceNumberReader,
        final FixPSequenceNumberHandler handler)
    {
        this.connectionIdToFixPSessionId = connectionIdToFixPSessionId;
        this.errorHandler = errorHandler;
        this.fixPProtocolType = fixPProtocolType;
        this.sequenceNumberReader = sequenceNumberReader;
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
            final int totalLength = BitUtil.align(srcLength, FRAME_ALIGNMENT);

            switch (templateId)
            {
                case FixPMessageDecoder.TEMPLATE_ID:
                {
                    onFixPMessage(
                        buffer, endPosition, offset, actingBlockLength, version, totalLength, header.sessionId());
                    break;
                }

                case ILinkConnectDecoder.TEMPLATE_ID:
                {
                    iLinkConnect.wrap(buffer, offset, actingBlockLength, version);
                    connectionIdToFixPSessionId.put(iLinkConnect.connection(), iLinkConnect.uuid());
                    break;
                }

                case InboundFixPConnectDecoder.TEMPLATE_ID:
                {
                    inboundFixPConnect.wrap(buffer, offset, actingBlockLength, version);
                    connectionIdToFixPSessionId.put(inboundFixPConnect.connection(), inboundFixPConnect.sessionId());
                    break;
                }

                case FollowerSessionRequestDecoder.TEMPLATE_ID:
                {
                    followerSessionRequest.wrap(buffer, offset, actingBlockLength, version);
                    if (followerSessionRequest.protocolType() == this.fixPProtocolType)
                    {
                        if (!lazyLoadSequenceExtractor(true))
                        {
                            sequenceExtractor.onFollowerSessionRequest(
                                followerSessionRequest, endPosition, totalLength, header.sessionId());
                        }
                    }
                    break;
                }
            }
        }
    }

    private void onFixPMessage(
        final DirectBuffer buffer,
        final long endPosition,
        final int offset,
        final int actingBlockLength,
        final int version,
        final int totalLength,
        final int aeronSessionId)
    {
        if (lazyLoadSequenceExtractor(true))
        {
            return;
        }

        fixPMessage.wrap(buffer, offset, actingBlockLength, version);
        final int sofhOffset = offset + FixPMessageDecoder.BLOCK_LENGTH;
        final int headerOffset = sofhOffset + SOFH_LENGTH;
        final long timestamp = fixPMessage.enqueueTime();

        sequenceExtractor.onMessage(
            fixPMessage,
            buffer,
            headerOffset,
            totalLength,
            endPosition,
            aeronSessionId,
            timestamp);
    }

    public void onRedactSequenceUpdate(final long sessionId, final int newSequenceNumber)
    {
        if (lazyLoadSequenceExtractor(false))
        {
            return;
        }

        sequenceExtractor.onRedactSequenceUpdate(sessionId, newSequenceNumber);
    }

    private boolean lazyLoadSequenceExtractor(final boolean logError)
    {
        if (!attemptedProtocolInit)
        {
            attemptedProtocolInit = true;

            final FixPProtocol protocol = FixPProtocolFactory.make(fixPProtocolType, logError ? errorHandler : null);
            if (protocol == null)
            {
                if (logError)
                {
                    errorHandler.onError(new IllegalStateException(
                        "Configuration Issue: could not setup Binary FIXP protocol on the Engine classpath, despite " +
                        "Binary FIXP message requiring processing. Sequence Index update ignored. " +
                        "If you're using iLink3 then you should be the artio-ilink3-codecs and artio-ilink3-impl" +
                        "dependencies on the classpath. " +
                        "Binary entrypoint requires a call to EngineConfiguration.acceptBinaryEntryPoint()"));
                }
                return true;
            }

            sequenceExtractor = protocol.makeSequenceExtractor(handler, sequenceNumberReader);
        }

        return sequenceExtractor == null;
    }
}
