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
package uk.co.real_logic.fix_gateway.protocol;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.messages.*;

import static uk.co.real_logic.fix_gateway.protocol.Streams.UNKNOWN_TEMPLATE;

public class EngineProtocolSubscription implements FragmentHandler
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final InitiateConnectionDecoder initiateConnection = new InitiateConnectionDecoder();
    private final RequestDisconnectDecoder requestDisconnect = new RequestDisconnectDecoder();
    private final ApplicationHeartbeatDecoder applicationHeartbeat = new ApplicationHeartbeatDecoder();
    private final LibraryConnectDecoder libraryConnect = new LibraryConnectDecoder();
    private final ReleaseSessionDecoder releaseSession = new ReleaseSessionDecoder();
    private final RequestSessionDecoder requestSession = new RequestSessionDecoder();

    private final EngineProtocolHandler handler;

    public EngineProtocolSubscription(final EngineProtocolHandler handler)
    {
        this.handler = handler;
    }

    public void onFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        readFragment(buffer, offset, header);
    }

    public int readFragment(final DirectBuffer buffer, int offset, final Header header)
    {
        messageHeader.wrap(buffer, offset);

        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();
        offset += messageHeader.encodedLength();

        switch (messageHeader.templateId())
        {
            case RequestDisconnectDecoder.TEMPLATE_ID:
            {
                return onRequestDisconnect(buffer, offset, blockLength, version);
            }

            case InitiateConnectionDecoder.TEMPLATE_ID:
            {
                return onInitiateConnection(buffer, offset, blockLength, version, header);
            }

            case ApplicationHeartbeatDecoder.TEMPLATE_ID:
            {
                return onApplicationHeartbeat(buffer, offset, blockLength, version);
            }

            case LibraryConnectDecoder.TEMPLATE_ID:
            {
                return onLibraryConnect(buffer, offset, blockLength, version, header);
            }

            case ReleaseSessionDecoder.TEMPLATE_ID:
            {
                return onReleaseSession(buffer, offset, blockLength, version, header);
            }

            case RequestSessionDecoder.TEMPLATE_ID:
            {
                return onRequestSession(buffer, offset, blockLength, version);
            }
        }

        return UNKNOWN_TEMPLATE;
    }

    private int onApplicationHeartbeat(final DirectBuffer buffer,
                                       final int offset,
                                       final int blockLength,
                                       final int version)
    {
        applicationHeartbeat.wrap(buffer, offset, blockLength, version);
        handler.onApplicationHeartbeat(applicationHeartbeat.libraryId());
        return applicationHeartbeat.limit();
    }


    private int onLibraryConnect(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version, final Header header)
    {
        libraryConnect.wrap(buffer, offset, blockLength, version);
        handler.onLibraryConnect(
            libraryConnect.libraryId(),
            header.sessionId());
        return libraryConnect.limit();
    }

    private int onReleaseSession(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version, final Header header)
    {
        releaseSession.wrap(buffer, offset, blockLength, version);
        handler.onReleaseSession(
            releaseSession.libraryId(),
            releaseSession.connection(),
            releaseSession.correlationId(),
            releaseSession.state(),
            releaseSession.heartbeatIntervalInMs(),
            releaseSession.lastSentSequenceNumber(),
            releaseSession.lastReceivedSequenceNumber(),
            releaseSession.username(),
            releaseSession.password(),
            header);
        return releaseSession.limit();
    }

    private int onRequestSession(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        requestSession.wrap(buffer, offset, blockLength, version);
        handler.onRequestSession(
            requestSession.libraryId(),
            requestSession.connection(),
            requestSession.correlationId(),
            requestSession.lastReceivedSequenceNumber());
        return requestSession.limit();
    }

    private int onInitiateConnection(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final Header header)
    {
        initiateConnection.wrap(buffer, offset, blockLength, version);
        handler.onInitiateConnection(
            initiateConnection.libraryId(),
            initiateConnection.port(),
            initiateConnection.host(),
            initiateConnection.senderCompId(),
            initiateConnection.senderSubId(),
            initiateConnection.senderLocationId(),
            initiateConnection.targetCompId(),
            initiateConnection.sequenceNumberType(),
            initiateConnection.requestedInitialSequenceNumber(),
            initiateConnection.username(),
            initiateConnection.password(),
            initiateConnection.heartbeatIntervalInS(),
            header
        );
        return initiateConnection.limit();
    }

    private int onRequestDisconnect(final DirectBuffer buffer,
                                    final int offset,
                                    final int blockLength,
                                    final int version)
    {
        requestDisconnect.wrap(buffer, offset, blockLength, version);
        handler.onRequestDisconnect(requestDisconnect.libraryId(), requestDisconnect.connection());
        return requestDisconnect.limit();
    }
}
