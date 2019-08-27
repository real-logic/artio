package uk.co.real_logic.artio.session;

import uk.co.real_logic.artio.fields.RejectReason;
import uk.co.real_logic.artio.messages.DisconnectReason;

public class SilentSessionProxy implements SessionProxy
{
    private static final long NOT_BACKPRESSURED = 1;

    public void setupSession(final long sessionId, final CompositeKey sessionKey)
    {
    }

    public long sendResendRequest(
        final int msgSeqNo,
        final int beginSeqNo, final int endSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendRequestDisconnect(final long connectionId, final DisconnectReason reason)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendLogon(
        final int msgSeqNo,
        final int heartbeatIntervalInS,
        final String username,
        final String password,
        final boolean resetSeqNumFlag, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendLogout(final int msgSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendLogout(
        final int msgSeqNo, final int sequenceIndex, final int rejectReason, final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendLowSequenceNumberLogout(
        final int msgSeqNo,
        final int expectedSeqNo,
        final int receivedSeqNo,
        final int sequenceIndex,
        final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendIncorrectBeginStringLogout(
        final int msgSeqNo,
        final int sequenceIndex,
        final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendNegativeHeartbeatLogout(
        final int msgSeqNo,
        final int sequenceIndex,
        final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendReceivedMessageWithoutSequenceNumber(
        final int msgSeqNo,
        final int sequenceIndex,
        final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendRejectWhilstNotLoggedOn(
        final int msgSeqNo, final RejectReason reason, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendHeartbeat(final int msgSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendHeartbeat(
        final int msgSeqNo,
        final char[] testReqId,
        final int testReqIdLength, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendReject(
        final int msgSeqNo,
        final int refSeqNum,
        final int refTagId,
        final char[] refMsgType,
        final int refMsgTypeLength, final int rejectReason, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendTestRequest(
        final int msgSeqNo,
        final CharSequence testReqID, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public long sendSequenceReset(
        final int msgSeqNo,
        final int newSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
    {
        return NOT_BACKPRESSURED;
    }

    public void libraryConnected(final boolean libraryConnected)
    {
    }

    public boolean seqNumResetRequested()
    {
        return false;
    }
}
