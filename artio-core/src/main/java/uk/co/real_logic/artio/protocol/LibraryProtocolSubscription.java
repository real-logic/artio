/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.artio.protocol;

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.messages.*;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

public final class LibraryProtocolSubscription implements ControlledFragmentHandler
{
    private static final int READ_META_DATA_META_DATA_PREFIX =
        ReadMetaDataReplyDecoder.BLOCK_LENGTH + ReadMetaDataReplyDecoder.metaDataHeaderLength();

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final ErrorDecoder error = new ErrorDecoder();
    private final ApplicationHeartbeatDecoder applicationHeartbeat = new ApplicationHeartbeatDecoder();
    private final ReleaseSessionReplyDecoder releaseSessionReply = new ReleaseSessionReplyDecoder();
    private final RequestSessionReplyDecoder requestSessionReply = new RequestSessionReplyDecoder();
    private final WriteMetaDataReplyDecoder writeMetaDataReply = new WriteMetaDataReplyDecoder();
    private final ReadMetaDataReplyDecoder readMetaDataReply = new ReadMetaDataReplyDecoder();
    private final ControlNotificationDecoder controlNotification = new ControlNotificationDecoder();
    private final SlowStatusNotificationDecoder slowStatusNotification = new SlowStatusNotificationDecoder();
    private final ResetLibrarySequenceNumberDecoder resetLibrarySequenceNumber =
        new ResetLibrarySequenceNumberDecoder();
    private final ManageSessionDecoder manageSession = new ManageSessionDecoder();
    private final FollowerSessionReplyDecoder followerSessionReply = new FollowerSessionReplyDecoder();
    private final EndOfDayDecoder endOfDay = new EndOfDayDecoder();
    private final ReplayMessagesReplyDecoder replayMessagesReply = new ReplayMessagesReplyDecoder();
    private final ILinkConnectDecoder iLinkConnect = new ILinkConnectDecoder();
    private final LibraryExtendPositionDecoder libraryExtendPosition = new LibraryExtendPositionDecoder();
    private final ReplayCompleteDecoder replayComplete = new ReplayCompleteDecoder();

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
            case ManageSessionDecoder.TEMPLATE_ID:
            {
                return onManageSession(buffer, offset, blockLength, version);
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

            case FollowerSessionReplyDecoder.TEMPLATE_ID:
            {
                return onFollowerSessionReply(buffer, offset, blockLength, version);
            }

            case WriteMetaDataReplyDecoder.TEMPLATE_ID:
            {
                return onWriteMetaDataReply(buffer, offset, blockLength, version);
            }

            case ReadMetaDataReplyDecoder.TEMPLATE_ID:
            {
                return onReadMetaDataReply(buffer, offset, blockLength, version);
            }

            case EndOfDayDecoder.TEMPLATE_ID:
            {
                return onEndOfDay(buffer, offset, blockLength, version);
            }

            case ReplayMessagesReplyDecoder.TEMPLATE_ID:
            {
                return onReplayMessagesReply(buffer, offset, blockLength, version);
            }

            case ILinkConnectDecoder.TEMPLATE_ID:
            {
                return onILinkConnect(buffer, offset, blockLength, version);
            }

            case LibraryExtendPositionDecoder.TEMPLATE_ID:
            {
                return onLibraryExtendPosition(buffer, offset, blockLength, version);
            }

            case ReplayCompleteDecoder.TEMPLATE_ID:
            {
                return onReplayComplete(buffer, offset, blockLength, version);
            }
        }

        return CONTINUE;
    }

    private Action onLibraryExtendPosition(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        libraryExtendPosition.wrap(buffer, offset, blockLength, version);

        // Deliberately don't use as a heartbeat signal.
        return handler.onLibraryExtendPosition(
            libraryExtendPosition.libraryId(),
            libraryExtendPosition.correlationId(),
            libraryExtendPosition.sessionId(),
            libraryExtendPosition.stopPosition(),
            libraryExtendPosition.initialTermId(),
            libraryExtendPosition.termBufferLength(),
            libraryExtendPosition.mtuLength());
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
            controlNotification.initialAcceptedSessionOwner(),
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

    private Action onFollowerSessionReply(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version)
    {
        followerSessionReply.wrap(buffer, offset, blockLength, version);
        final int libraryId = followerSessionReply.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }

        return handler.onFollowerSessionReply(
            libraryId,
            followerSessionReply.replyToId(),
            followerSessionReply.session());
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

    private Action onWriteMetaDataReply(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        writeMetaDataReply.wrap(buffer, offset, blockLength, version);
        final int libraryId = writeMetaDataReply.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }

        return handler.onWriteMetaDataReply(
            libraryId,
            writeMetaDataReply.replyToId(),
            writeMetaDataReply.status());
    }

    private Action onReadMetaDataReply(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        readMetaDataReply.wrap(buffer, offset, blockLength, version);
        final int libraryId = readMetaDataReply.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }

        return handler.onReadMetaDataReply(
            libraryId,
            readMetaDataReply.replyToId(),
            readMetaDataReply.status(),
            buffer,
            offset + READ_META_DATA_META_DATA_PREFIX,
            readMetaDataReply.metaDataLength());
    }

    private Action onILinkConnect(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        iLinkConnect.wrap(buffer, offset, blockLength, version);
        final int libraryId = iLinkConnect.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }

        return handler.onILinkConnect(
            libraryId,
            iLinkConnect.correlationId(),
            iLinkConnect.connection(),
            iLinkConnect.uuid(),
            iLinkConnect.lastReceivedSequenceNumber(),
            iLinkConnect.lastSentSequenceNumber(),
            iLinkConnect.newlyAllocated() == Bool.TRUE,
            iLinkConnect.lastUuid());
    }

    private Action onReplayMessagesReply(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        replayMessagesReply.wrap(buffer, offset, blockLength, version);
        final int libraryId = replayMessagesReply.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);
        if (action == ABORT)
        {
            return action;
        }

        return handler.onReplayMessagesReply(
            libraryId,
            replayMessagesReply.replyToId(),
            replayMessagesReply.status());
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

    private Action onManageSession(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version)
    {
        manageSession.wrap(buffer, offset, blockLength, version);
        final int libraryId = manageSession.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);

        if (ABORT == action)
        {
            return action;
        }

        return handler.onManageSession(
            libraryId,
            manageSession.connection(),
            manageSession.session(),
            manageSession.lastSentSequenceNumber(),
            manageSession.lastReceivedSequenceNumber(),
            manageSession.sessionStatus(),
            manageSession.slowStatus(),
            manageSession.connectionType(),
            manageSession.sessionState(),
            manageSession.heartbeatIntervalInS(),
            manageSession.closedResendInterval() == Bool.TRUE,
            manageSession.resendRequestChunkSize(),
            manageSession.sendRedundantResendRequests() == Bool.TRUE,
            manageSession.enableLastMsgSeqNumProcessed() == Bool.TRUE,
            manageSession.replyToId(),
            manageSession.sequenceIndex(),
            manageSession.awaitingResend() == AwaitingResend.YES,
            manageSession.lastResentMsgSeqNo(),
            manageSession.lastResendChunkMsgSeqNum(),
            manageSession.endOfResendRequestRange(),
            manageSession.awaitingHeartbeat() == Bool.TRUE,
            manageSession.logonReceivedSequenceNumber(),
            manageSession.logonSequenceIndex(),
            manageSession.lastLogonTime(),
            manageSession.lastSequenceResetTime(),
            manageSession.localCompId(),
            manageSession.localSubId(),
            manageSession.localLocationId(),
            manageSession.remoteCompId(),
            manageSession.remoteSubId(),
            manageSession.remoteLocationId(),
            manageSession.address(),
            manageSession.username(),
            manageSession.password(),
            FixDictionary.find(manageSession.fixDictionary()),
            manageSession.metaDataStatus(),
            buffer,
            manageSession.limit() + ManageSessionDecoder.metaDataHeaderLength(),
            manageSession.metaDataLength());
    }

    private Action onEndOfDay(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version)
    {
        endOfDay.wrap(buffer, offset, blockLength, version);
        final int libraryId = endOfDay.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);

        if (ABORT == action)
        {
            return action;
        }

        return handler.onEngineClose(libraryId);
    }

    private Action onReplayComplete(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version)
    {
        replayComplete.wrap(buffer, offset, blockLength, version);
        final long connection = replayComplete.connection();
        final int libraryId = replayComplete.libraryId();
        final Action action = handler.onApplicationHeartbeat(libraryId);

        if (ABORT == action)
        {
            return action;
        }

        return handler.onReplayComplete(libraryId, connection);
    }


}
