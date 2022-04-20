/*
 * Copyright 2015-2022 Real Logic Limited.
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

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.messages.*;

public interface LibraryEndPointHandler
{
    Action onError(int libraryId, GatewayError errorType, long replyToId, String message);

    Action onApplicationHeartbeat(int libraryId, int messageTemplateId, long timestampInNs);

    Action onReleaseSessionReply(int libraryId, long replyToId, SessionReplyStatus status);

    Action onRequestSessionReply(int toId, long replyToId, SessionReplyStatus status);

    Action onControlNotification(
        int libraryId, InitialAcceptedSessionOwner initialAcceptedSessionOwner,
        ControlNotificationDecoder controlNotification);

    Action onSlowStatusNotification(int libraryId, long connectionId, boolean hasBecomeSlow);

    Action onResetLibrarySequenceNumber(int libraryId, long sessionId);

    Action onManageSession(
        int libraryId,
        long connection,
        long session,
        int lastSentSeqNum,
        int lastRecvSeqNum,
        SessionStatus sessionStatus,
        SlowStatus slowStatus,
        ConnectionType connectionType,
        SessionState sessionState,
        int heartBeatInt,
        boolean closedResendInterval,
        int resendRequestChunkSize,
        boolean sendRedundantResendRequests,
        boolean enableLastMsgSeqNumProcessed,
        long correlationId,
        int sequenceIndex,
        boolean awaitingResend,
        int lastResentMsgSeqNo,
        int lastResendChunkMsgSeqNum,
        int endOfResendRequestRange,
        boolean awaitingHeartbeat,
        int logonReceivedSequenceNumber,
        int logonSequenceIndex,
        long lastLogonTime,
        long lastSequenceResetTime,
        String localCompId,
        String localSubId,
        String localLocationId,
        String remoteCompId,
        String remoteSubId,
        String remoteLocationId,
        String address,
        String username,
        String password,
        Class<? extends FixDictionary> fixDictionary,
        MetaDataStatus metaDataStatus,
        DirectBuffer metaDataBuffer,
        int metaDataOffset,
        int metaDataLength,
        CancelOnDisconnectOption cancelOnDisconnectOption,
        long cancelOnDisconnectTimeoutInNs);

    Action onFollowerSessionReply(int libraryId, long replyToId, long session);

    Action onEngineClose(int libraryId);

    Action onWriteMetaDataReply(int libraryId, long replyToId, MetaDataStatus status);

    Action onReadMetaDataReply(
        int libraryId,
        long replyToId,
        MetaDataStatus status,
        DirectBuffer srcBuffer,
        int srcOffset,
        int srcLength);

    Action onReplayMessagesReply(int libraryId, long replyToId, ReplayMessagesStatus status);

    Action onILinkConnect(
        int libraryId,
        long correlationId,
        long connection,
        long uuid,
        long lastReceivedSequenceNumber,
        long lastSentSequenceNumber,
        boolean newlyAllocated,
        long lastUuid);

    Action onLibraryExtendPosition(
        int libraryId, long correlationId, int newSessionId, long stopPosition, int initialTermId,
        int termBufferLength, int mtuLength);

    Action onReplayComplete(int libraryId, long connection, long correlationId);

    Action onInboundFixPConnect(
        long connection,
        long sessionId,
        FixPProtocolType protocol,
        DirectBuffer buffer,
        int limit,
        int messageLength);

    Action onManageFixPConnection(
        int libraryId,
        long correlationId,
        long connection,
        long sessionId,
        FixPProtocolType protocolType,
        long lastReceivedSequenceNumber,
        long lastSentSequenceNumber,
        long lastConnectPayload,
        boolean offline,
        DirectBuffer buffer,
        int limit,
        int messageLength);

    Action onThrottleNotification(
        int libraryId,
        long connection,
        long refMsgType,
        int refSeqNum,
        DirectBuffer businessRejectRefIDBuffer, int businessRejectRefIDOffset, int businessRejectRefIDLength,
        long position);

    Action onThrottleConfigurationReply(int libraryId, long replyToId, ThrottleConfigurationStatus status);
}
