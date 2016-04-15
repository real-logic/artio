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

import static uk.co.real_logic.fix_gateway.messages.ManageConnectionDecoder.addressHeaderLength;
import static uk.co.real_logic.fix_gateway.protocol.Streams.UNKNOWN_TEMPLATE;

public class LibraryProtocolSubscription implements FragmentHandler
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final LogonDecoder logon = new LogonDecoder();
    private final ConnectDecoder connect = new ConnectDecoder();
    private final ManageConnectionDecoder manageConnection = new ManageConnectionDecoder();
    private final ErrorDecoder error = new ErrorDecoder();
    private final ApplicationHeartbeatDecoder applicationHeartbeat = new ApplicationHeartbeatDecoder();
    private final ReleaseSessionReplyDecoder releaseSessionReply = new ReleaseSessionReplyDecoder();
    private final RequestSessionReplyDecoder requestSessionReply = new RequestSessionReplyDecoder();
    private final CatchupDecoder catchup = new CatchupDecoder();
    private final NewSentPositionDecoder newSentPosition = new NewSentPositionDecoder();

    private final LibraryProtocolHandler handler;

    public LibraryProtocolSubscription(final LibraryProtocolHandler handler)
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
            case NewSentPositionDecoder.TEMPLATE_ID:
            {
                return onNewSentPosition(buffer, offset, blockLength, version);
            }

            case LogonDecoder.TEMPLATE_ID:
            {
                return onLogon(buffer, offset, blockLength, version);
            }

            case ManageConnectionDecoder.TEMPLATE_ID:
            {
                return onManageConnection(buffer, offset, blockLength, version);
            }

            case ErrorDecoder.TEMPLATE_ID:
            {
                return onError(buffer, offset, blockLength, version);
            }

            case ApplicationHeartbeatDecoder.TEMPLATE_ID:
            {
                return onApplicationHeartbeat(buffer, offset, blockLength, version);
            }

            case ReleaseSessionReplyDecoder.TEMPLATE_ID:
            {
                return onReleaseSessionReply(buffer, offset, blockLength, version);
            }

            case RequestSessionReplyDecoder.TEMPLATE_ID:
            {
                return onRequestSessionReply(buffer, offset, blockLength, version);
            }

            case ConnectDecoder.TEMPLATE_ID:
            {
                return onConnect(buffer, offset, blockLength, version);
            }

            case CatchupDecoder.TEMPLATE_ID:
            {
                return onCatchup(buffer, offset, blockLength, version);
            }
        }

        return UNKNOWN_TEMPLATE;
    }

    private int onCatchup(final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        catchup.wrap(buffer, offset, blockLength, version);
        handler.onCatchup(
            catchup.libraryId(),
            catchup.connection(),
            catchup.messageCount()
        );
        return catchup.limit();
    }

    private int onConnect(final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        connect.wrap(buffer, offset, blockLength, version);
        handler.onConnect(
            connect.connection(),
            connect.address()
        );
        return connect.limit();
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

    private int onReleaseSessionReply(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        releaseSessionReply.wrap(buffer, offset, blockLength, version);
        handler.onReleaseSessionReply(
            releaseSessionReply.correlationId(),
            releaseSessionReply.status());
        return releaseSessionReply.limit();
    }

    private int onRequestSessionReply(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        requestSessionReply.wrap(buffer, offset, blockLength, version);
        handler.onRequestSessionReply(
            requestSessionReply.correlationId(),
            requestSessionReply.status());
        return requestSessionReply.limit();
    }

    private int onError(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        error.wrap(buffer, offset, blockLength, version);
        handler.onError(
            error.type(),
            error.libraryId(),
            error.message()
        );

        return error.limit();
    }

    private int onNewSentPosition(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        newSentPosition.wrap(buffer, offset, blockLength, version);
        handler.onNewSentPosition(
            newSentPosition.position()
        );

        return error.limit();
    }

    private int onManageConnection(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        manageConnection.wrap(buffer, offset, blockLength, version);
        final int addressOffset = offset + ManageConnectionDecoder.BLOCK_LENGTH + addressHeaderLength();
        handler.onManageConnection(
            manageConnection.libraryId(),
            manageConnection.connection(),
            manageConnection.type(),
            manageConnection.lastSentSequenceNumber(),
            manageConnection.lastReceivedSequenceNumber(),
            buffer,
            addressOffset,
            manageConnection.addressLength(),
            manageConnection.sessionState());
        return manageConnection.limit();
    }

    private int onLogon(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        logon.wrap(buffer, offset, blockLength, version);
        handler.onLogon(
            logon.libraryId(),
            logon.connection(),
            logon.session(),
            logon.lastSentSequenceNumber(),
            logon.lastReceivedSequenceNumber(),
            logon.senderCompId(),
            logon.senderSubId(),
            logon.senderLocationId(),
            logon.targetCompId(),
            logon.username(),
            logon.password());
        return logon.limit();
    }
}
