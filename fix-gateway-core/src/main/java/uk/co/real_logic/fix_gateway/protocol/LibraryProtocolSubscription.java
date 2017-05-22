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

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.messages.ManageConnectionDecoder.addressHeaderLength;

public final class LibraryProtocolSubscription implements ControlledFragmentHandler
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final SessionExistsDecoder sessionExists = new SessionExistsDecoder();
    private final ManageConnectionDecoder manageConnection = new ManageConnectionDecoder();
    private final ErrorDecoder error = new ErrorDecoder();
    private final ApplicationHeartbeatDecoder applicationHeartbeat = new ApplicationHeartbeatDecoder();
    private final ReleaseSessionReplyDecoder releaseSessionReply = new ReleaseSessionReplyDecoder();
    private final RequestSessionReplyDecoder requestSessionReply = new RequestSessionReplyDecoder();
    private final NewSentPositionDecoder newSentPosition = new NewSentPositionDecoder();
    private final NotLeaderDecoder libraryConnect = new NotLeaderDecoder();
    private final ControlNotificationDecoder controlNotification = new ControlNotificationDecoder();
    private final SlowStatusNotificationDecoder slowStatusNotification = new SlowStatusNotificationDecoder();
    private final ResetLibrarySequenceNumberDecoder resetLibrarySequenceNumber =
        new ResetLibrarySequenceNumberDecoder();

    private final LibraryEndPointHandler handler;

    public LibraryProtocolSubscription(final LibraryEndPointHandler handler)
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
            case NewSentPositionDecoder.TEMPLATE_ID:
            {
                return onNewSentPosition(buffer, offset, blockLength, version);
            }

            case SessionExistsDecoder.TEMPLATE_ID:
            {
                return onSessionExists(buffer, offset, blockLength, version);
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

            case NotLeaderDecoder.TEMPLATE_ID:
            {
                return onNotLeader(buffer, offset, blockLength, version);
            }

            case ControlNotificationDecoder.TEMPLATE_ID:
            {
                return onControlNotification(buffer, offset, blockLength, version);
            }

            case SlowStatusNotificationDecoder.TEMPLATE_ID:
            {
                return onSlowStatusNotification(buffer, offset, blockLength, version);
            }

            case ResetLibrarySequenceNumberDecoder.TEMPLATE_ID:
            {
                return onResetLibrarySequenceNumber(buffer, offset, blockLength, version);
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
        // Deliberately don't keepalive the heartbeat - may not be a cluster leader

        return handler.onNotLeader(
            libraryConnect.libraryId(),
            libraryConnect.replyToId(),
            libraryConnect.libraryChannel());
    }

    private Action onControlNotification(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version)
    {
        controlNotification.wrap(buffer, offset, blockLength, version);
        final int libraryId = controlNotification.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }

        return handler.onControlNotification(
            libraryId,
            controlNotification.sessions());
    }

    private Action onSlowStatusNotification(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version)
    {
        slowStatusNotification.wrap(buffer, offset, blockLength, version);
        final int libraryId = slowStatusNotification.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }

        return handler.onSlowStatusNotification(
            libraryId,
            slowStatusNotification.connectionId(),
            slowStatusNotification.status() == SlowStatus.SLOW);
    }

    private Action onResetLibrarySequenceNumber(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version)
    {
        resetLibrarySequenceNumber.wrap(buffer, offset, blockLength, version);
        final int libraryId = resetLibrarySequenceNumber.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }

        return handler.onResetLibrarySequenceNumber(
            libraryId,
            resetLibrarySequenceNumber.session());
    }

    private Action onApplicationHeartbeat(
        final DirectBuffer buffer,
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
        final int libraryId = releaseSessionReply.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }

        return handler.onReleaseSessionReply(
            libraryId,
            releaseSessionReply.replyToId(),
            releaseSessionReply.status());
    }

    private Action onRequestSessionReply(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        requestSessionReply.wrap(buffer, offset, blockLength, version);
        final int libraryId = requestSessionReply.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }

        return handler.onRequestSessionReply(
            libraryId,
            requestSessionReply.replyToId(),
            requestSessionReply.status());
    }

    private Action onError(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        error.wrap(buffer, offset, blockLength, version);
        final int libraryId = error.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }
        return handler.onError(
            libraryId,
            error.errorType(),
            error.replyToId(),
            error.message());
    }

    private Action onNewSentPosition(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        newSentPosition.wrap(buffer, offset, blockLength, version);
        // Deliberately don't keepalive the heartbeat - may not be a cluster leader

        return handler.onNewSentPosition(
            newSentPosition.libraryId(),
            newSentPosition.position());
    }

    private Action onManageConnection(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        manageConnection.wrap(buffer, offset, blockLength, version);
        final int addressOffset = offset + ManageConnectionDecoder.BLOCK_LENGTH + addressHeaderLength();
        final int libraryId = manageConnection.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }
        return handler.onManageConnection(
            libraryId,
            manageConnection.connection(),
            manageConnection.session(),
            manageConnection.connectionType(),
            manageConnection.lastSentSequenceNumber(),
            manageConnection.lastReceivedSequenceNumber(),
            buffer,
            addressOffset,
            manageConnection.addressLength(),
            manageConnection.sessionState(),
            manageConnection.heartbeatIntervalInS(),
            manageConnection.replyToId(),
            manageConnection.sequenceIndex());
    }

    private Action onSessionExists(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        sessionExists.wrap(buffer, offset, blockLength, version);
        final int libraryId = sessionExists.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }

        return handler.onSessionExists(
            libraryId,
            sessionExists.connection(),
            sessionExists.session(),
            sessionExists.lastSentSequenceNumber(),
            sessionExists.lastReceivedSequenceNumber(),
            sessionExists.logonStatus(),
            sessionExists.slowStatus() == SlowStatus.SLOW,
            sessionExists.localCompId(),
            sessionExists.localSubId(),
            sessionExists.localLocationId(),
            sessionExists.remoteCompId(),
            sessionExists.remoteSubId(),
            sessionExists.remoteLocationId(),
            sessionExists.username(),
            sessionExists.password());
    }
}
