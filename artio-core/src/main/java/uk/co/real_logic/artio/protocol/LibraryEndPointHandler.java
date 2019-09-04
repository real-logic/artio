/*
 * Copyright 2015-2017 Real Logic Ltd.
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
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.messages.ControlNotificationDecoder.SessionsDecoder;

public interface LibraryEndPointHandler
{
    Action onError(int libraryId, GatewayError errorType, long replyToId, String message);

    Action onApplicationHeartbeat(int libraryId);

    Action onReleaseSessionReply(int libraryId, long replyToId, SessionReplyStatus status);

    Action onRequestSessionReply(int toId, long replyToId, SessionReplyStatus status);

    Action onNewSentPosition(int libraryId, long position);

    Action onControlNotification(int libraryId, SessionsDecoder sessions);

    Action onSlowStatusNotification(int libraryId, long connectionId, boolean hasBecomeSlow);

    Action onResetLibrarySequenceNumber(int libraryId, long sessionId);

    Action onManageSession(
        int libraryId,
        long connection,
        long session,
        int lastSentSeqNum,
        int lastRecvSeqNum,
        long logonTime,
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
        String localCompId,
        String localSubId,
        String localLocationId,
        String remoteCompId,
        String remoteSubId,
        String remoteLocationId,
        String address,
        String username,
        String password,
        Class<? extends FixDictionary> fixDictionary);

    Action onFollowerSessionReply(int libraryId, long replyToId, long session);

    Action onEndOfDay(int libraryId);
}
