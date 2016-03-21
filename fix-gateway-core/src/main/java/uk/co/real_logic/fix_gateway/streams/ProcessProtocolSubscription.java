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
import uk.co.real_logic.fix_gateway.library.session.ProcessProtocolHandler;
import uk.co.real_logic.fix_gateway.messages.*;

import static uk.co.real_logic.fix_gateway.messages.ConnectDecoder.addressHeaderLength;
import static uk.co.real_logic.fix_gateway.streams.Streams.UNKNOWN_TEMPLATE;

public class ProcessProtocolSubscription implements FragmentHandler
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final LogonDecoder logon = new LogonDecoder();
    private final ConnectDecoder connect = new ConnectDecoder();
    private final InitiateConnectionDecoder initiateConnection = new InitiateConnectionDecoder();
    private final RequestDisconnectDecoder requestDisconnect = new RequestDisconnectDecoder();
    private final ErrorDecoder error = new ErrorDecoder();
    private final ApplicationHeartbeatDecoder applicationHeartbeat = new ApplicationHeartbeatDecoder();
    private final LibraryConnectDecoder libraryConnect = new LibraryConnectDecoder();
    private final ReleaseSessionDecoder releaseSession = new ReleaseSessionDecoder();
    private final ReleaseSessionReplyDecoder releaseSessionReply = new ReleaseSessionReplyDecoder();
    private final RequestSessionDecoder requestSession = new RequestSessionDecoder();
    private final RequestSessionReplyDecoder requestSessionReply = new RequestSessionReplyDecoder();

    private final ProcessProtocolHandler processProtocolHandler;

    public ProcessProtocolSubscription(final ProcessProtocolHandler processProtocolHandler)
    {
        this.processProtocolHandler = processProtocolHandler;
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
            case LogonDecoder.TEMPLATE_ID:
            {
                return onLogon(buffer, offset, blockLength, version);
            }

            case ConnectDecoder.TEMPLATE_ID:
            {
                return onConnect(buffer, offset, blockLength, version);
            }

            case RequestDisconnectDecoder.TEMPLATE_ID:
            {
                return onRequestDisconnect(buffer, offset, blockLength, version);
            }

            case InitiateConnectionDecoder.TEMPLATE_ID:
            {
                return onInitiateConnection(buffer, offset, blockLength, version, header);
            }

            case ErrorDecoder.TEMPLATE_ID:
            {
                return onError(buffer, offset, blockLength, version);
            }

            case ApplicationHeartbeatDecoder.TEMPLATE_ID:
            {
                return onApplicationHeartbeat(buffer, offset, blockLength, version);
            }

            case LibraryConnectDecoder.TEMPLATE_ID:
            {
                return onLibraryConnect(buffer, offset, blockLength, version);
            }

            case ReleaseSessionDecoder.TEMPLATE_ID:
            {
                return onReleaseSession(buffer, offset, blockLength, version);
            }

            case ReleaseSessionReplyDecoder.TEMPLATE_ID:
            {
                return onReleaseSessionReply(buffer, offset, blockLength, version);
            }

            case RequestSessionDecoder.TEMPLATE_ID:
            {
                return onRequestSession(buffer, offset, blockLength, version);
            }

            case RequestSessionReplyDecoder.TEMPLATE_ID:
            {
                return onRequestSessionReply(buffer, offset, blockLength, version);
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
        processProtocolHandler.onApplicationHeartbeat(applicationHeartbeat.libraryId());
        return applicationHeartbeat.limit();
    }


    private int onLibraryConnect(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        libraryConnect.wrap(buffer, offset, blockLength, version);
        processProtocolHandler.onLibraryConnect(libraryConnect.libraryId(), libraryConnect.typeHandled());
        return libraryConnect.limit();
    }

    private int onReleaseSession(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        releaseSession.wrap(buffer, offset, blockLength, version);
        processProtocolHandler.onReleaseSession(
            releaseSession.libraryId(),
            releaseSession.connection(),
            releaseSession.correlationId());
        return releaseSession.limit();
    }

    private int onReleaseSessionReply(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        releaseSessionReply.wrap(buffer, offset, blockLength, version);
        processProtocolHandler.onReleaseSessionReply(
            releaseSessionReply.correlationId(),
            releaseSessionReply.status());
        return releaseSessionReply.limit();
    }

    private int onRequestSession(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        requestSession.wrap(buffer, offset, blockLength, version);
        processProtocolHandler.onRequestSession(
            requestSession.libraryId(),
            requestSession.connection(),
            requestSession.correlationId());
        return requestSession.limit();
    }

    private int onRequestSessionReply(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        requestSessionReply.wrap(buffer, offset, blockLength, version);
        processProtocolHandler.onRequestSessionReply(
            requestSessionReply.correlationId(),
            requestSessionReply.status());
        return requestSessionReply.limit();
    }

    private int onError(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        error.wrap(buffer, offset, blockLength, version);
        processProtocolHandler.onError(
            error.type(),
            error.libraryId(),
            error.message()
        );

        return error.limit();
    }

    private int onInitiateConnection(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final Header header)
    {
        initiateConnection.wrap(buffer, offset, blockLength, version);
        processProtocolHandler.onInitiateConnection(
            initiateConnection.libraryId(),
            initiateConnection.port(),
            initiateConnection.host(),
            initiateConnection.senderCompId(),
            initiateConnection.senderSubId(),
            initiateConnection.senderLocationId(),
            initiateConnection.targetCompId(),
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
        processProtocolHandler.onRequestDisconnect(requestDisconnect.libraryId(), requestDisconnect.connection());
        return requestDisconnect.limit();
    }

    private int onConnect(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        connect.wrap(buffer, offset, blockLength, version);
        final int addressOffset = offset + ConnectDecoder.BLOCK_LENGTH + addressHeaderLength();
        processProtocolHandler.onConnect(
            connect.libraryId(),
            connect.connection(),
            connect.type(),
            connect.lastSentSequenceNumber(),
            connect.lastReceivedSequenceNumber(),
            buffer,
            addressOffset,
            connect.addressLength());
        return connect.limit();
    }

    private int onLogon(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        logon.wrap(buffer, offset, blockLength, version);
        processProtocolHandler.onLogon(
            logon.libraryId(),
            logon.connection(),
            logon.session(),
            logon.lastSentSequenceNumber(),
            logon.lastReceivedSequenceNumber());
        return logon.limit();
    }
}
