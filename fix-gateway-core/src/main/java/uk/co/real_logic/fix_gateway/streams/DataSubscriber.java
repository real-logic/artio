/*
 * Copyright 2015 Real Logic Ltd.
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
import uk.co.real_logic.fix_gateway.library.session.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.*;

import static uk.co.real_logic.fix_gateway.messages.ConnectDecoder.addressHeaderLength;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.OK;
import static uk.co.real_logic.fix_gateway.streams.GatewayPublication.FRAME_SIZE;

public class DataSubscriber implements FragmentHandler
{
    public static final int UNKNOWN_TEMPLATE = -1;

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final LogonDecoder logon = new LogonDecoder();
    private final ConnectDecoder connect = new ConnectDecoder();
    private final InitiateConnectionDecoder initiateConnection = new InitiateConnectionDecoder();
    private final RequestDisconnectDecoder requestDisconnect = new RequestDisconnectDecoder();
    private final DisconnectDecoder disconnect = new DisconnectDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();
    private final ErrorDecoder error = new ErrorDecoder();
    private final ApplicationHeartbeatDecoder applicationHeartbeat = new ApplicationHeartbeatDecoder();
    private final LibraryConnectDecoder libraryConnect = new LibraryConnectDecoder();

    private final SessionHandler sessionHandler;

    public DataSubscriber(final SessionHandler sessionHandler)
    {
        this.sessionHandler = sessionHandler;
    }

    public void onFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        readFragment(buffer, offset);
    }

    public int readFragment(final DirectBuffer buffer, int offset)
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
                return onInitiateConnection(buffer, offset, blockLength, version);
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
        }

        return UNKNOWN_TEMPLATE;
    }

    private int onApplicationHeartbeat(final DirectBuffer buffer,
                                       final int offset,
                                       final int blockLength,
                                       final int version)
    {
        applicationHeartbeat.wrap(buffer, offset, blockLength, version);
        sessionHandler.onApplicationHeartbeat(applicationHeartbeat.libraryId());
        return applicationHeartbeat.limit();
    }


    private int onLibraryConnect(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        libraryConnect.wrap(buffer, offset, blockLength, version);
        sessionHandler.onLibraryConnect(libraryConnect.libraryId(), libraryConnect.typeHandled());
        return libraryConnect.limit();
    }

    private int onError(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        error.wrap(buffer, offset, blockLength, version);
        sessionHandler.onError(
            error.type(),
            error.libraryId(),
            error.message()
        );

        return error.limit();
    }

    private int onInitiateConnection(final DirectBuffer buffer,
                                     final int offset,
                                     final int blockLength,
                                     final int version)
    {
        initiateConnection.wrap(buffer, offset, blockLength, version);
        sessionHandler.onInitiateConnection(
            initiateConnection.libraryId(),
            initiateConnection.port(),
            initiateConnection.host(),
            initiateConnection.senderCompId(),
            initiateConnection.senderSubId(),
            initiateConnection.senderLocationId(),
            initiateConnection.targetCompId()
        );
        return initiateConnection.limit();
    }

    private int onRequestDisconnect(final DirectBuffer buffer,
                                    final int offset,
                                    final int blockLength,
                                    final int version)
    {
        requestDisconnect.wrap(buffer, offset, blockLength, version);
        sessionHandler.onRequestDisconnect(requestDisconnect.libraryId(), requestDisconnect.connection());
        return requestDisconnect.limit();
    }

    private int onConnect(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        connect.wrap(buffer, offset, blockLength, version);
        final int addressOffset = offset + ConnectDecoder.BLOCK_LENGTH + addressHeaderLength();
        sessionHandler.onConnect(
            connect.libraryId(),
            connect.connection(),
            connect.type(),
            connect.lastSequenceNumber(),
            buffer,
            addressOffset,
            connect.addressLength());
        return connect.limit();
    }

    private int onLogon(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        logon.wrap(buffer, offset, blockLength, version);
        sessionHandler.onLogon(logon.libraryId(), logon.connection(), logon.session());
        return logon.limit();
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
