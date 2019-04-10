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
        int heartbeatIntervalInS,
        int msgSeqNo,
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
        char[] testReqId,
        int testReqIdLength,
        int msgSeqNo,
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
