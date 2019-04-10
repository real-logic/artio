/*
 * Copyright 2015-2019 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.session;

import uk.co.real_logic.artio.fields.RejectReason;
import uk.co.real_logic.artio.messages.DisconnectReason;

public interface SessionProxy
{
    void setupSession(long sessionId, CompositeKey sessionKey);

    long resendRequest(
        int msgSeqNo,
        int beginSeqNo,
        int endSeqNo,
        int sequenceIndex,
        int lastMsgSeqNumProcessed);

    long requestDisconnect(long connectionId, DisconnectReason reason);

    long logon(
        int msgSeqNo,
        int heartbeatIntervalInS,
        String username,
        String password,
        boolean resetSeqNumFlag,
        int sequenceIndex,
        int lastMsgSeqNumProcessed);

    long logout(int msgSeqNo, int sequenceIndex, int lastMsgSeqNumProcessed);

    long logout(
        int msgSeqNo, int sequenceIndex, int rejectReason, int lastMsgSeqNumProcessed);

    long lowSequenceNumberLogout(
        int msgSeqNo,
        int expectedSeqNo,
        int receivedSeqNo,
        int sequenceIndex,
        int lastMsgSeqNumProcessed);

    long incorrectBeginStringLogout(
        int msgSeqNo, int sequenceIndex, int lastMsgSeqNumProcessed);

    long negativeHeartbeatLogout(
        int msgSeqNo, int sequenceIndex, int lastMsgSeqNumProcessed);

    long receivedMessageWithoutSequenceNumber(
        int msgSeqNo, int sequenceIndex, int lastMsgSeqNumProcessed);

    long rejectWhilstNotLoggedOn(
        int msgSeqNo, RejectReason reason, int sequenceIndex, int lastMsgSeqNumProcessed);

    long heartbeat(int msgSeqNo, int sequenceIndex, int lastMsgSeqNumProcessed);

    long heartbeat(
        int msgSeqNo,
        char[] testReqId,
        int testReqIdLength,
        int sequenceIndex,
        int lastMsgSeqNumProcessed);

    long reject(
        int msgSeqNo,
        int refSeqNum,
        int refTagId,
        char[] refMsgType,
        int refMsgTypeLength,
        int rejectReason,
        int sequenceIndex,
        int lastMsgSeqNumProcessed);

    long testRequest(
        int msgSeqNo, CharSequence testReqID, int sequenceIndex, int lastMsgSeqNumProcessed);

    long sequenceReset(
        int msgSeqNo, int newSeqNo, int sequenceIndex, int lastMsgSeqNumProcessed);

    void libraryConnected(boolean libraryConnected);

    boolean seqNumResetRequested();
}
