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

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.messages.*;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.messages.ManageConnectionDecoder.addressHeaderLength;

public final class LibraryProtocolSubscription implements ControlledFragmentHandler
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final LogonDecoder logon = new LogonDecoder();
    private final ManageConnectionDecoder manageConnection = new ManageConnectionDecoder();
    private final ErrorDecoder error = new ErrorDecoder();
    private final ApplicationHeartbeatDecoder applicationHeartbeat = new ApplicationHeartbeatDecoder();
    private final ReleaseSessionReplyDecoder releaseSessionReply = new ReleaseSessionReplyDecoder();
    private final RequestSessionReplyDecoder requestSessionReply = new RequestSessionReplyDecoder();
    private final CatchupDecoder catchup = new CatchupDecoder();
    private final NewSentPositionDecoder newSentPosition = new NewSentPositionDecoder();
    private final NotLeaderDecoder libraryConnect = new NotLeaderDecoder();
    private final ControlNotificationDecoder controlNotification = new ControlNotificationDecoder();

    private final LibraryEndPointHandler handler;

    public LibraryProtocolSubscription(final LibraryEndPointHandler handler)
    {
        this.handler = handler;
    }

    public Action onFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
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

            case CatchupDecoder.TEMPLATE_ID:
            {
                return onCatchup(buffer, offset, blockLength, version);
            }

            case NotLeaderDecoder.TEMPLATE_ID:
            {
                return onNotLeader(buffer, offset, blockLength, version);
            }

            case ControlNotificationDecoder.TEMPLATE_ID:
            {
                return onControlNotification(buffer, offset, blockLength, version);
            }
        }

        return CONTINUE;
    }

    private Action onNotLeader(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version)
    {
        libraryConnect.wrap(buffer, offset, blockLength, version);
        return handler.onNotLeader(
            libraryConnect.libraryId(),
            libraryConnect.libraryChannel()
        );
    }

    private Action onControlNotification(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version)
    {
        controlNotification.wrap(buffer, offset, blockLength, version);
        return handler.onControlNotification(
            controlNotification.libraryId(),
            controlNotification.sessions()
        );
    }

    private Action onCatchup(final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        catchup.wrap(buffer, offset, blockLength, version);
        return handler.onCatchup(
            catchup.libraryId(),
            catchup.connection(),
            catchup.messageCount()
        );
    }

    private Action onApplicationHeartbeat(final DirectBuffer buffer,
                                       final int offset,
                                       final int blockLength,
                                       final int version)
    {
        applicationHeartbeat.wrap(buffer, offset, blockLength, version);
        return handler.onApplicationHeartbeat(applicationHeartbeat.libraryId());
    }

    private Action onReleaseSessionReply(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        releaseSessionReply.wrap(buffer, offset, blockLength, version);
        return handler.onReleaseSessionReply(
            releaseSessionReply.correlationId(),
            releaseSessionReply.status());
    }

    private Action onRequestSessionReply(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        requestSessionReply.wrap(buffer, offset, blockLength, version);
        return handler.onRequestSessionReply(
            requestSessionReply.correlationId(),
            requestSessionReply.status());
    }

    private Action onError(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        error.wrap(buffer, offset, blockLength, version);
        return handler.onError(
            error.type(),
            error.libraryId(),
            error.replyToId(),
            error.message()
        );
    }

    private Action onNewSentPosition(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        newSentPosition.wrap(buffer, offset, blockLength, version);
        return handler.onNewSentPosition(
            newSentPosition.libraryId(),
            newSentPosition.position()
        );
    }

    private Action onManageConnection(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        manageConnection.wrap(buffer, offset, blockLength, version);
        final int addressOffset = offset + ManageConnectionDecoder.BLOCK_LENGTH + addressHeaderLength();
        return handler.onManageConnection(
            manageConnection.libraryId(),
            manageConnection.connection(),
            manageConnection.session(),
            manageConnection.type(),
            manageConnection.lastSentSequenceNumber(),
            manageConnection.lastReceivedSequenceNumber(),
            buffer,
            addressOffset,
            manageConnection.addressLength(),
            manageConnection.sessionState(),
            manageConnection.heartbeatIntervalInS(),
            manageConnection.replyToId());
    }

    private Action onLogon(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        logon.wrap(buffer, offset, blockLength, version);
        return handler.onLogon(
            logon.libraryId(),
            logon.connection(),
            logon.session(),
            logon.lastSentSequenceNumber(),
            logon.lastReceivedSequenceNumber(),
            logon.status(),
            logon.senderCompId(),
            logon.senderSubId(),
            logon.senderLocationId(),
            logon.targetCompId(),
            logon.username(),
            logon.password());
    }
}
