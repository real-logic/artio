/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.protocol;

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.messages.*;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

public final class EngineProtocolSubscription implements ControlledFragmentHandler
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final InitiateConnectionDecoder initiateConnection = new InitiateConnectionDecoder();
    private final RequestDisconnectDecoder requestDisconnect = new RequestDisconnectDecoder();
    private final MidConnectionDisconnectDecoder midConnectionDisconnect = new MidConnectionDisconnectDecoder();
    private final ApplicationHeartbeatDecoder applicationHeartbeat = new ApplicationHeartbeatDecoder();
    private final LibraryConnectDecoder libraryConnect = new LibraryConnectDecoder();
    private final ReleaseSessionDecoder releaseSession = new ReleaseSessionDecoder();
    private final RequestSessionDecoder requestSession = new RequestSessionDecoder();

    private final EngineEndPointHandler handler;

    public EngineProtocolSubscription(final EngineEndPointHandler handler)
    {
        this.handler = handler;
    }

    @SuppressWarnings("FinalParameters")
    public Action onFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        messageHeader.wrap(buffer, offset);

        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();
        offset += messageHeader.encodedLength();

        switch (messageHeader.templateId())
        {
            case RequestDisconnectDecoder.TEMPLATE_ID:
            {
                return onRequestDisconnect(buffer, offset, blockLength, version, header);
            }

            case InitiateConnectionDecoder.TEMPLATE_ID:
            {
                return onInitiateConnection(buffer, offset, blockLength, version, header);
            }

            case ApplicationHeartbeatDecoder.TEMPLATE_ID:
            {
                return onApplicationHeartbeat(buffer, offset, blockLength, version, header);
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
                return onRequestSession(buffer, offset, blockLength, version, header);
            }

            case MidConnectionDisconnectDecoder.TEMPLATE_ID:
            {
                return onMidConnectionDisconnect(buffer, offset, blockLength, version, header);
            }
        }

        return CONTINUE;
    }

    private Action onApplicationHeartbeat(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final Header header)
    {
        applicationHeartbeat.wrap(buffer, offset, blockLength, version);
        handler.onApplicationHeartbeat(applicationHeartbeat.libraryId(), header.sessionId());
        return CONTINUE;
    }

    private Action onLibraryConnect(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final Header header)
    {
        libraryConnect.wrap(buffer, offset, blockLength, version);
        final int libraryId = libraryConnect.libraryId();
        final String libraryName = libraryConnect.libraryName();

        return handler.onLibraryConnect(
            libraryId,
            libraryName,
            libraryConnect.correlationId(),
            header.sessionId());
    }

    private Action onReleaseSession(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final Header header)
    {
        releaseSession.wrap(buffer, offset, blockLength, version);
        final int libraryId = releaseSession.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId, header.sessionId());
        if (action != null)
        {
            return action; // Continue processing messages, but not this message.
        }
        return handler.onReleaseSession(
            libraryId,
            releaseSession.connection(),
            releaseSession.sessionId(),
            releaseSession.correlationId(),
            releaseSession.state(),
            releaseSession.awaitingResend() == AwaitingResend.YES,
            releaseSession.heartbeatIntervalInMs(),
            releaseSession.lastSentSequenceNumber(),
            releaseSession.lastReceivedSequenceNumber(),
            releaseSession.username(),
            releaseSession.password(),
            header);
    }

    private Action onRequestSession(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final Header header)
    {
        requestSession.wrap(buffer, offset, blockLength, version);
        final int libraryId = requestSession.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId, header.sessionId());
        if (action != null)
        {
            return action; // Continue processing messages but not this message.
        }

        return handler.onRequestSession(
            libraryId,
            requestSession.sessionId(),
            requestSession.correlationId(),
            requestSession.lastReceivedSequenceNumber(),
            requestSession.sequenceIndex());
    }

    private Action onInitiateConnection(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final Header header)
    {
        initiateConnection.wrap(buffer, offset, blockLength, version);
        final int libraryId = initiateConnection.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId, header.sessionId());
        if (action != null)
        {
            return action; // Continue processing messages, but don't process this message.
        }
        return handler.onInitiateConnection(
            libraryId,
            initiateConnection.port(),
            initiateConnection.host(),
            initiateConnection.senderCompId(),
            initiateConnection.senderSubId(),
            initiateConnection.senderLocationId(),
            initiateConnection.targetCompId(),
            initiateConnection.targetSubId(),
            initiateConnection.targetLocationId(),
            initiateConnection.sequenceNumberType(),
            initiateConnection.requestedInitialReceivedSequenceNumber(),
            initiateConnection.requestedInitialSentSequenceNumber(),
            initiateConnection.resetSequenceNumber() == ResetSequenceNumber.YES,
            initiateConnection.closedResendInterval() == Bool.TRUE,
            initiateConnection.resendRequestChunkSize(),
            initiateConnection.sendRedundantResendRequests() == Bool.TRUE,
            initiateConnection.enableLastMsgSeqNumProcessed() == Bool.TRUE,
            initiateConnection.username(),
            initiateConnection.password(),
            initiateConnection.heartbeatIntervalInS(),
            initiateConnection.correlationId(),
            header
        );
    }

    private Action onRequestDisconnect(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final Header header)
    {
        requestDisconnect.wrap(buffer, offset, blockLength, version);
        final int libraryId = requestDisconnect.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId, header.sessionId());
        if (action != null)
        {
            return action; // Continue processing messages, but not this message.
        }
        return handler.onRequestDisconnect(
            libraryId,
            requestDisconnect.connection(),
            requestDisconnect.reason());
    }

    private Action onMidConnectionDisconnect(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final Header header)
    {
        midConnectionDisconnect.wrap(buffer, offset, blockLength, version);
        final int libraryId = midConnectionDisconnect.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId, header.sessionId());
        if (action != null)
        {
            return action; // Continue processing messages, but not this message.
        }
        return handler.onMidConnectionDisconnect(
            libraryId,
            midConnectionDisconnect.correlationId());
    }
}
