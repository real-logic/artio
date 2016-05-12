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
package uk.co.real_logic.fix_gateway.session;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Test;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.decoder.SequenceResetDecoder;
import uk.co.real_logic.fix_gateway.engine.framer.FakeEpochClock;
import uk.co.real_logic.fix_gateway.messages.SessionState;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.decoder.Constants.NEW_SEQ_NO;
import static uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.fix_gateway.fields.RejectReason.*;
import static uk.co.real_logic.fix_gateway.messages.SessionState.*;
import static uk.co.real_logic.fix_gateway.session.Session.TEST_REQ_ID;
import static uk.co.real_logic.fix_gateway.session.Session.UNKNOWN;

public abstract class AbstractSessionTest
{
    public static final long SESSION_ID = 2L;

    static final long TWO_MINUTES = MINUTES.toMillis(2);
    static final long SENDING_TIME_WINDOW = 2000;
    static final long CONNECTION_ID = 3L;
    static final int HEARTBEAT_INTERVAL = 2;
    static final CompositeKey SESSION_KEY = mock(CompositeKey.class);
    static final int LIBRARY_ID = 4;

    private static final byte[] MSG_TYPE_BYTES = "D".getBytes(US_ASCII);
    private static final long POSITION = 1024;

    SessionProxy mockProxy = mock(SessionProxy.class);
    GatewayPublication mockPublication = mock(GatewayPublication.class);
    FakeEpochClock fakeClock = new FakeEpochClock();
    AtomicCounter mockReceivedMsgSeqNo = mock(AtomicCounter.class);
    AtomicCounter mockSentMsgSeqNo = mock(AtomicCounter.class);

    void verifyNoFurtherMessages()
    {
        verifyNoMoreInteractions(mockProxy);
    }

    @Test
    public void shouldLogoutOnLowSequenceNumber()
    {
        givenActive();
        session().lastReceivedMsgSeqNum(2);

        onMessage(1);
        verify(mockProxy).lowSequenceNumberLogout(1, 3, 1);
        verifyDisconnect(1);
    }

    @Test
    public void shouldDisconnectIfMissingSequenceNumber()
    {
        onLogon(1);

        final int nextMsgSeqNum = nextMsgSeqNum();

        onMessage(MISSING_INT);

        receivedMessageWithoutSequenceNumber(nextMsgSeqNum, 1);
        verifyDisconnect(1);
    }

    @Test
    public void shouldDisconnectIfMissingSequenceNumberWhenBackPressured()
    {
        onLogon(1);

        final int nextMsgSeqNum = nextMsgSeqNum();

        when(mockProxy.receivedMessageWithoutSequenceNumber(nextMsgSeqNum))
            .thenReturn(BACK_PRESSURED, POSITION);

        assertEquals(ABORT, onMessage(MISSING_INT));

        assertEquals(CONTINUE, onMessage(MISSING_INT));

        receivedMessageWithoutSequenceNumber(nextMsgSeqNum, 2);
        verifyDisconnect(1);
    }

    private int nextMsgSeqNum()
    {
        return session().lastSentMsgSeqNum() + 1;
    }

    private void receivedMessageWithoutSequenceNumber(final int sentMsgSeqNum, final int times)
    {
        verify(mockProxy, times(times)).receivedMessageWithoutSequenceNumber(sentMsgSeqNum);
    }

    @Test
    public void shouldLogoutIfNegativeHeartbeatInterval()
    {
        readyForLogon();

        final int heartbeatInterval = -1;

        session().onLogon(
            heartbeatInterval, 1, SESSION_ID, SESSION_KEY, fakeClock.time(), UNKNOWN, null, null, false);

        verify(mockProxy).negativeHeartbeatLogout(1);
    }

    @Test
    public void shouldValidateOriginalSendingTimeBeforeSendingTime()
    {
        final long sendingTime = sendingTime();
        final long origSendingTime = sendingTime + 10;

        onLogon(1);

        onMessage(2);

        session().onMessage(2, MSG_TYPE_BYTES, sendingTime, origSendingTime, true);

        verifySendingTimeProblem();
    }

    @Test
    public void shouldValidateOriginalSendingTimeExistsIfPossDupFlagIsSet()
    {
        onLogon(1);

        onMessage(2);

        session().onMessage(2, MSG_TYPE_BYTES, sendingTime(), UNKNOWN, true);

        verify(mockProxy).reject(2, 2, MSG_TYPE_BYTES, MSG_TYPE_BYTES.length, REQUIRED_TAG_MISSING);
    }

    @Test
    public void shouldNotifyClientUponSequenceReset()
    {
        final int newSeqNo = 10;

        onLogon(1);

        assertThat(session().lastSentMsgSeqNum(), lessThanOrEqualTo(1));

        session().sequenceReset(newSeqNo);

        verify(mockProxy).sequenceReset(anyInt(), eq(newSeqNo));
        assertEquals(newSeqNo - 1, session().lastSentMsgSeqNum());
    }

    @Test
    public void shouldSendHeartbeatAfterLogonSpecifiedInterval()
    {
        readyForLogon();

        final int heartbeatInterval = 1;
        session().onLogon(heartbeatInterval, 1, SESSION_ID, null, fakeClock.time(), UNKNOWN, null, null, false);

        heartbeatSentAfterInterval(heartbeatInterval, 2, false);
    }

    @Test
    public void shouldSendHeartbeatAfterInterval()
    {
        shouldSendHeartbeatAfterInterval(false);
    }

    @Test
    public void shouldSendHeartbeatAfterIntervalWhenBackPressured()
    {
        shouldSendHeartbeatAfterInterval(true);
    }

    private void shouldSendHeartbeatAfterInterval(final boolean backPressured)
    {
        readyForLogon();

        onLogon(1);

        heartbeatSentAfterInterval(1, 2, backPressured);
    }

    @Test
    public void shouldSendHeartbeatsAfterIntervalRepeatedly()
    {
        shouldSendHeartbeatAfterInterval();

        heartbeatSentAfterInterval(2, 3, false);

        heartbeatSentAfterInterval(3, 4, false);
    }

    @Test
    public void shouldSendHeartbeatsAfterIntervalRepeatedlyWhenBackPressured()
    {
        shouldSendHeartbeatAfterInterval(true);

        heartbeatSentAfterInterval(2, 3, true);

        heartbeatSentAfterInterval(3, 4, true);
    }

    @Test
    public void shouldReplyToValidLogout()
    {
        givenActive();

        onLogout();

        verifyLogout();
        verifyDisconnect(1);
    }

    @Test
    public void shouldDisconnectUponLogoutAcknowledgement()
    {
        session().state(AWAITING_LOGOUT);

        onLogout();

        verifyDisconnect(1);
    }

    @Test
    public void shouldReplyToTestRequestsWithAHeartbeat()
    {
        final char[] testReqId = "ABC".toCharArray();
        final int testReqIdLength = testReqId.length;

        session().id(SESSION_ID);

        session().onTestRequest(1, testReqId, testReqIdLength, sendingTime(), UNKNOWN, false);

        verify(mockProxy).heartbeat(testReqId, testReqIdLength, 1);
    }

    @Test
    public void shouldResendRequestForUnexpectedGapFill()
    {
        session().id(SESSION_ID);

        session().onSequenceReset(3, 4, true, false);
        onMessage(3);

        verify(mockProxy).resendRequest(1, 1, 0);
    }

    @Test
    public void shouldIgnoreDuplicateGapFill()
    {
        session().lastReceivedMsgSeqNum(2);

        session().onSequenceReset(1, 4, false, true);

        verifyNoFurtherMessages();
    }

    @Test
    public void shouldLogoutOnInvalidGapFill()
    {
        session().lastReceivedMsgSeqNum(2);

        session().onSequenceReset(1, 4, true, false);

        verify(mockProxy).lowSequenceNumberLogout(anyInt(), eq(3), eq(1));
        verifyDisconnect(1);
    }

    @Test
    public void shouldUpdateSequenceNumberOnValidGapFill()
    {
        givenActive();

        session().onSequenceReset(1, 4, true, false);

        assertEquals(4, session().expectedReceivedSeqNum());
        verifyNoFurtherMessages();
        verifyConnected();

        verifyCanRoundtripTestMessage();
    }

    @Test
    public void shouldIgnoreMsgSeqNumWithoutGapFillFlag()
    {
        givenActive();

        session().onSequenceReset(0, 4, false, false);

        assertEquals(4, session().expectedReceivedSeqNum());
        verifyNoFurtherMessages();
        verifyConnected();

        verifyCanRoundtripTestMessage();
    }

    @Test
    public void shouldUpdateSequenceNumberOnSequenceReset()
    {
        session().onSequenceReset(4, 4, false, false);

        assertEquals(4, session().expectedReceivedSeqNum());
        verifyNoFurtherMessages();
    }

    @Test
    public void shouldAcceptUnnecessarySequenceReset()
    {
        session().lastReceivedMsgSeqNum(3);

        session().onSequenceReset(4, 4, false, false);

        assertEquals(4, session().expectedReceivedSeqNum());
        verifyNoFurtherMessages();
    }

    @Test
    public void shouldRejectLowSequenceReset()
    {
        session().lastReceivedMsgSeqNum(3);

        session().onSequenceReset(2, 1, false, false);

        assertEquals(4, session().expectedReceivedSeqNum());
        verify(mockProxy).reject(
            1,
            2,
            NEW_SEQ_NO,
            SequenceResetDecoder.MESSAGE_TYPE_BYTES,
            SequenceResetDecoder.MESSAGE_TYPE_BYTES.length,
            VALUE_IS_INCORRECT);
    }

    @Test
    public void shouldSendTestRequestUponTimeout()
    {
        shouldSendTestRequestUponTimeout(false);
    }

    private void shouldSendTestRequestUponTimeout(final boolean backPressured)
    {
        givenActive();
        session().lastSentMsgSeqNum(5);
        session().lastReceivedMsgSeqNum(9);

        onMessage(10);

        twoHeartBeatIntervalsPass();

        if (backPressured)
        {
            when(mockProxy.testRequest(7, TEST_REQ_ID)).thenReturn(BACK_PRESSURED, POSITION);
        }

        poll();

        if (backPressured)
        {
            poll();
        }

        verify(mockProxy, retry(backPressured)).testRequest(7, TEST_REQ_ID);
        assertEquals(AWAITING_RESEND, session().state());
    }

    @Test
    public void shouldSendTestRequestUponTimeoutWhenBackPressured()
    {
        shouldSendTestRequestUponTimeout(true);
    }

    @Test
    public void shouldLogoutAndDisconnectUponTimeout()
    {
        shouldSendTestRequestUponTimeout();

        twoHeartBeatIntervalsPass();

        poll();

        verifyDisconnect(1);
    }

    @Test
    public void shouldLogoutAndDisconnectUponTimeoutWhenBackPressured()
    {
        shouldSendTestRequestUponTimeout();

        twoHeartBeatIntervalsPass();

        backpressureDisconnect();

        poll();

        assertState(DISCONNECTING);

        poll();

        verifyDisconnect(2);
    }

    @Test
    public void shouldSuppressTimeoutWhenMessageReceived()
    {
        givenActive();
        session().lastReceivedMsgSeqNum(9);

        onMessage(10);

        fakeClock.advanceSeconds(1);

        poll();
        onMessage(11);

        fakeClock.advanceSeconds(1);

        poll();

        verifyConnected();
    }

    @Test
    public void shouldRequestResendIfHighSeqNo()
    {
        givenActive();
        session().id(SESSION_ID);

        onMessage(3);

        verify(mockProxy).resendRequest(1, 1, 0);
        assertState(AWAITING_RESEND);
    }

    @Test
    public void shouldDisconnectIfBeginStringIsInvalidAtLogon()
    {
        onBeginString(true);
        verifyDisconnect(1);
    }

    @Test
    public void shouldDisconnectIfBeginStringIsInvalid()
    {
        onBeginString(false);
        incorrectBeginStringLogout(1);
        verifyDisconnect(1);
    }

    @Test
    public void shouldDisconnectIfBeginStringIsInvalidWhenBackPressured()
    {
        when(mockProxy.incorrectBeginStringLogout(1)).thenReturn(BACK_PRESSURED, POSITION);
        backpressureDisconnect();

        onBeginString(false);

        assertState(DISCONNECTING);

        poll();
        poll();

        incorrectBeginStringLogout(2);
        verifyDisconnect(2);
    }

    private void incorrectBeginStringLogout(final int times)
    {
        verify(mockProxy, times(times)).incorrectBeginStringLogout(1);
    }

    private void heartbeatSentAfterInterval(
        final int heartbeatInterval,
        final int recvMsgSeqNo,
        final boolean backPressured)
    {
        if (backPressured)
        {
            when(mockProxy.heartbeat(anyInt())).thenReturn(BACK_PRESSURED, POSITION);
        }

        final int sentMsgSeqNo = nextMsgSeqNum();

        fakeClock.advanceSeconds(heartbeatInterval);

        onMessage(recvMsgSeqNo);

        fakeClock.advanceSeconds(1);

        poll();

        if (backPressured)
        {
            poll();
        }

        verify(mockProxy, retry(backPressured)).heartbeat(sentMsgSeqNo);
        reset(mockProxy);
    }

    private VerificationMode retry(final boolean backPressured)
    {
        return times(backPressured ? 2 : 1);
    }

    public void verifyDisconnect(final int times)
    {
        verify(mockProxy, times(times)).requestDisconnect(CONNECTION_ID);
        assertState(DISCONNECTED);
    }

    public void verifyLogoutStarted()
    {
        verifyLogout();
        awaitingLogout();
    }

    private void backpressureDisconnect()
    {
        when(mockProxy.requestDisconnect(CONNECTION_ID)).thenReturn(BACK_PRESSURED, POSITION);
    }

    protected void givenActive()
    {
        session().state(ACTIVE);
    }

    public void awaitingLogout()
    {
        assertState(AWAITING_LOGOUT);
    }

    public void verifyLogout()
    {
        verify(mockProxy).logout(anyInt());
    }

    public void assertState(final SessionState state)
    {
        assertEquals(state, session().state());
    }

    public void onLogon(final int msgSeqNo)
    {
        session().onLogon(HEARTBEAT_INTERVAL, msgSeqNo, SESSION_ID, SESSION_KEY, fakeClock.time(), UNKNOWN, null, null, false);
    }

    protected Action onMessage(final int msgSeqNo)
    {
        return session().onMessage(msgSeqNo, MSG_TYPE_BYTES, sendingTime(), UNKNOWN, false);
    }

    protected long sendingTime()
    {
        return fakeClock.time() - 1;
    }

    protected void onLogout()
    {
        session().onLogout(1, sendingTime(), UNKNOWN, false);
    }

    protected void verifySendingTimeProblem()
    {
        verify(mockProxy).reject(2, 2, MSG_TYPE_BYTES, MSG_TYPE_BYTES.length, SENDINGTIME_ACCURACY_PROBLEM);
    }

    protected void messageWithWeirdTime(final long sendingTime)
    {
        session().onMessage(2, MSG_TYPE_BYTES, sendingTime, UNKNOWN, false);
    }

    protected void onBeginString(final boolean isLogon)
    {
        final char[] beginString = "FIX.3.9 ".toCharArray();
        final boolean valid = session().onBeginString(beginString, beginString.length - 1, isLogon);
        assertFalse(valid);
    }

    protected void twoHeartBeatIntervalsPass()
    {
        fakeClock.advanceSeconds(HEARTBEAT_INTERVAL * 2);
    }

    protected void verifyConnected()
    {
        verify(mockProxy, never()).requestDisconnect(CONNECTION_ID);
    }

    protected void verifyCanRoundtripTestMessage()
    {
        final char[] testReqId = "Hello".toCharArray();
        final int testReqIdLength = 5;

        session().onTestRequest(4, testReqId, testReqIdLength, sendingTime(), UNKNOWN, false);
        verify(mockProxy).heartbeat(eq(testReqId), eq(testReqIdLength), anyInt());
        verifyConnected();
    }

    protected void poll()
    {
        session().poll(fakeClock.time());
    }

    protected abstract Session session();

    protected abstract void readyForLogon();
}
