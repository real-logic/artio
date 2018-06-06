/*
 * Copyright 2015-2017 Real Logic Ltd.
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

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.artio.builder.HeaderEncoder;
import uk.co.real_logic.artio.builder.TestRequestEncoder;
import uk.co.real_logic.artio.decoder.SequenceResetDecoder;
import uk.co.real_logic.artio.engine.framer.FakeEpochClock;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.CommonConfiguration.DEFAULT_REASONABLE_TRANSMISSION_TIME_IN_S;
import static uk.co.real_logic.artio.Constants.NEW_SEQ_NO;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.artio.fields.RejectReason.*;
import static uk.co.real_logic.artio.messages.DisconnectReason.APPLICATION_DISCONNECT;
import static uk.co.real_logic.artio.messages.SessionState.*;
import static uk.co.real_logic.artio.session.Session.TEST_REQ_ID;
import static uk.co.real_logic.artio.session.Session.UNKNOWN;

public abstract class AbstractSessionTest
{
    public static final long SESSION_ID = 2L;

    static final long TWO_MINUTES = MINUTES.toMillis(2);
    static final long SENDING_TIME_WINDOW = 2000;
    static final long CONNECTION_ID = 3L;
    static final int HEARTBEAT_INTERVAL = 2;
    static final int SESSION_TIMEOUT = HEARTBEAT_INTERVAL + DEFAULT_REASONABLE_TRANSMISSION_TIME_IN_S;
    static final CompositeKey SESSION_KEY = mock(CompositeKey.class);
    static final int LIBRARY_ID = 4;
    static final int SEQUENCE_INDEX = 0;

    private static final byte[] MSG_TYPE_BYTES = "D".getBytes(US_ASCII);

    static final long POSITION = 1024;

    SessionProxy mockProxy = mock(SessionProxy.class);
    GatewayPublication mockPublication = mock(GatewayPublication.class);
    FakeEpochClock fakeClock = new FakeEpochClock();
    AtomicCounter mockReceivedMsgSeqNo = mock(AtomicCounter.class);
    AtomicCounter mockSentMsgSeqNo = mock(AtomicCounter.class);
    SessionIdStrategy idStrategy = mock(SessionIdStrategy.class);
    ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
    ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);
    TestRequestEncoder testRequest = new TestRequestEncoder();
    SessionLogonListener mockLogonListener = mock(SessionLogonListener.class);


    AbstractSessionTest()
    {
        doAnswer(
            (inv) ->
            {
                final HeaderEncoder encoder = (HeaderEncoder)inv.getArguments()[1];
                encoder.senderCompID("senderCompID").targetCompID("targetCompID");
                return null;
            }).when(idStrategy).setupSession(any(), any());

        when(mockPublication.saveMessage(
            bufferCaptor.capture(),
            offsetCaptor.capture(),
            lengthCaptor.capture(),
            anyInt(),
            anyInt(),
            anyLong(),
            anyInt(),
            anyLong(),
            any()
        )).thenReturn(POSITION);
    }

    @Test
    public void shouldLogoutOnLowSequenceNumber()
    {
        givenActive();
        session().lastReceivedMsgSeqNum(2);

        onMessage(1);
        verify(mockProxy).lowSequenceNumberLogout(1, 3, 1, SEQUENCE_INDEX);
        verifyDisconnect(times(1));
    }

    @Test
    public void shouldDisconnectIfMissingSequenceNumber()
    {
        onLogon(1);

        final int nextMsgSeqNum = nextMsgSeqNum();

        assertEquals(CONTINUE, onMessage(MISSING_INT));

        receivedMessageWithoutSequenceNumber(nextMsgSeqNum, 1);
        verifyDisconnect(times(1));
    }

    @Test
    public void shouldDisconnectIfMissingSequenceNumberWhenBackPressured()
    {
        onLogon(1);

        final int nextMsgSeqNum = nextMsgSeqNum();

        when(mockProxy.receivedMessageWithoutSequenceNumber(nextMsgSeqNum, SEQUENCE_INDEX))
            .thenReturn(BACK_PRESSURED, POSITION);

        assertEquals(ABORT, onMessage(MISSING_INT));

        assertEquals(CONTINUE, onMessage(MISSING_INT));

        receivedMessageWithoutSequenceNumber(nextMsgSeqNum, 2);
        verifyDisconnect(times(1));
    }

    private int nextMsgSeqNum()
    {
        return session().lastSentMsgSeqNum() + 1;
    }

    private void receivedMessageWithoutSequenceNumber(final int sentMsgSeqNum, final int times)
    {
        verify(mockProxy, times(times)).receivedMessageWithoutSequenceNumber(sentMsgSeqNum, SEQUENCE_INDEX);
    }

    @Test
    public void shouldLogoutIfNegativeHeartbeatInterval()
    {
        readyForLogon();

        final int heartbeatInterval = -1;

        onLogon(heartbeatInterval, 1, false);

        verify(mockProxy).negativeHeartbeatLogout(1, SEQUENCE_INDEX);
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
        assertSequenceIndexIs(SEQUENCE_INDEX);
    }

    @Test
    public void shouldValidateOriginalSendingTimeExistsIfPossDupFlagIsSet()
    {
        onLogon(1);

        onMessage(2);

        session().onMessage(2, MSG_TYPE_BYTES, sendingTime(), UNKNOWN, true);

        verify(mockProxy).reject(2, 2, MSG_TYPE_BYTES, MSG_TYPE_BYTES.length, REQUIRED_TAG_MISSING, SEQUENCE_INDEX);
    }

    @Test
    public void shouldNotifyClientUponSequenceReset()
    {
        final int newSeqNo = 10;

        onLogon(1);

        assertThat(session().lastSentMsgSeqNum(), lessThanOrEqualTo(1));

        session().sendSequenceReset(newSeqNo);

        final int nextSequenceIndex = SEQUENCE_INDEX + 1;
        verify(mockProxy).sequenceReset(anyInt(), eq(newSeqNo), eq(nextSequenceIndex));
        assertEquals(newSeqNo - 1, session().lastSentMsgSeqNum());
        assertSequenceIndexIs(nextSequenceIndex);
    }

    @Test
    public void shouldSendHeartbeatAfterLogonSpecifiedInterval()
    {
        readyForLogon();

        final int heartbeatInterval = 1;
        onLogon(heartbeatInterval, 1, false);

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

        verifyLogout(1, times(1));
        verifyDisconnect(times(1));
    }

    @Test
    public void shouldReplyToValidLogoutWhenBackPressured()
    {
        givenActive();

        backPressureLogout();

        onLogout();

        poll();

        poll();

        poll();

        verifyLogout(1, times(2));
        verifyDisconnect(times(2));
    }

    private void backPressureLogout()
    {
        when(mockProxy.logout(anyInt(), eq(SEQUENCE_INDEX))).thenReturn(BACK_PRESSURED, POSITION);

        backPressureDisconnect();
    }

    @Test
    public void shouldDisconnectUponLogoutAcknowledgement()
    {
        session().state(AWAITING_LOGOUT);

        onLogout();

        verifyDisconnect(times(1));
    }

    @Test
    public void shouldReplyToTestRequestsWithAHeartbeat()
    {
        final char[] testReqId = "ABC".toCharArray();
        final int testReqIdLength = testReqId.length;

        session().id(SESSION_ID);

        session().onTestRequest(1, testReqId, testReqIdLength, sendingTime(), UNKNOWN, false);

        verify(mockProxy).heartbeat(testReqId, testReqIdLength, 1, SEQUENCE_INDEX);
    }

    @Test
    public void shouldResendRequestForUnexpectedGapFill()
    {
        session().id(SESSION_ID);

        onSequenceReset();

        verify(mockProxy).resendRequest(1, 1, 0, SEQUENCE_INDEX);
    }

    @Test
    public void shouldResendRequestForUnexpectedGapFillWhenBackPressured()
    {
        backPressureResendRequest();

        session().id(SESSION_ID);

        session().lastSentMsgSeqNum(70);
        assertEquals(ABORT, onSequenceReset());
        assertEquals(CONTINUE, onSequenceReset());

        assertEquals(71, session().lastSentMsgSeqNum());
        assertEquals(3, session().lastReceivedMsgSeqNum());

        verify(mockProxy, times(2)).resendRequest(71, 1, 0, SEQUENCE_INDEX);
    }

    private Action onSequenceReset()
    {
        return session().onSequenceReset(3, 4, true, false);
    }

    private void backPressureResendRequest()
    {
        when(mockProxy.resendRequest(anyInt(), anyInt(), anyInt(), eq(SEQUENCE_INDEX)))
            .thenReturn(BACK_PRESSURED, POSITION);
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

        verify(mockProxy).lowSequenceNumberLogout(anyInt(), eq(3), eq(1), eq(SEQUENCE_INDEX));
        verifyDisconnect(times(1));
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
        assertSequenceIndexIs(SEQUENCE_INDEX);
    }

    @Test
    public void shouldAcceptUnnecessarySequenceReset()
    {
        session().lastReceivedMsgSeqNum(3);

        session().onSequenceReset(4, 4, false, false);

        assertEquals(4, session().expectedReceivedSeqNum());
        verifyNoFurtherMessages();
        assertSequenceIndexIs(SEQUENCE_INDEX);
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
            VALUE_IS_INCORRECT, SEQUENCE_INDEX);
        assertSequenceIndexIs(SEQUENCE_INDEX);
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
            when(mockProxy.testRequest(7, TEST_REQ_ID, SEQUENCE_INDEX)).thenReturn(BACK_PRESSURED, POSITION);
        }

        poll();

        if (backPressured)
        {
            poll();
        }

        verify(mockProxy, retry(backPressured)).testRequest(7, TEST_REQ_ID, SEQUENCE_INDEX);
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

        verifyDisconnect(times(1));
    }

    @Test
    public void shouldLogoutAndDisconnectUponTimeoutWhenBackPressured()
    {
        shouldSendTestRequestUponTimeout();

        twoHeartBeatIntervalsPass();

        backPressureDisconnect();

        poll();

        assertState(DISCONNECTING);

        poll();

        verifyDisconnect(times(2));
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

        verify(mockProxy).resendRequest(1, 1, 0, SEQUENCE_INDEX);
        assertState(AWAITING_RESEND);
    }

    @Test
    public void shouldDisconnectIfBeginStringIsInvalidAtLogon()
    {
        onBeginString(true);
        verifyDisconnect(times(1));
    }

    @Test
    public void shouldDisconnectIfBeginStringIsInvalid()
    {
        onBeginString(false);
        incorrectBeginStringLogout(1);
        verifyDisconnect(times(1));
    }

    @Test
    public void shouldDisconnectIfBeginStringIsInvalidWhenBackPressured()
    {
        when(mockProxy.incorrectBeginStringLogout(1, SEQUENCE_INDEX)).thenReturn(BACK_PRESSURED, POSITION);
        backPressureDisconnect();

        onBeginString(false);

        assertState(DISCONNECTING);

        poll();
        poll();

        incorrectBeginStringLogout(2);
        verifyDisconnect(times(2));
    }

    @Test
    public void shouldStartLogonBasedSequenceNumberReset()
    {
        sequenceNumbersAreThreeAndActive();

        session().resetSequenceNumbers();

        verifySetsSentSequenceNumbersToTwo(SEQUENCE_INDEX + 1);
    }

    public void shouldStartAcceptLogonBasedSequenceNumberResetWhenSequenceNumberIsOne(final int sequenceIndex)
    {
        onLogon(HEARTBEAT_INTERVAL, 1, true);

        verifySetupSession();
        verifySetsSequenceNumbersToTwo(sequenceIndex);
    }

    @Test
    public void shouldComplyWithLogonBasedSequenceNumberReset()
    {
        int sequenceIndex = SEQUENCE_INDEX;
        for (final SessionState state : SessionState.values())
        {
            Mockito.reset(mockProxy);

            session().state(state);
            sequenceNumbersAreThree();

            onLogon(HEARTBEAT_INTERVAL, 1, true);

            session().poll(100);

            sequenceIndex++;
            verifySetupSession();
            verifySetsSequenceNumbersToTwo(sequenceIndex);
            assertSequenceIndexIs(sequenceIndex);
        }
    }

    @Test
    public void shouldTerminateLogonBasedSequenceNumberReset()
    {
        shouldStartLogonBasedSequenceNumberReset();

        onLogon(HEARTBEAT_INTERVAL, 1, true);

        verifySetupSession();
        verifyNoFurtherMessages();
        assertSequenceIndexIs(SEQUENCE_INDEX + 1);
    }

    @Test
    public void shouldCorrectEncodeMessageTimestamps()
    {
        givenActive();

        final String message = sendTestRequest(0);

        assertThat(message, containsString(":00\001"));
    }

    @Test
    public void shouldCorrectEncodeMessageTimestampsRepeatedly()
    {
        givenActive();

        final long nonSecondDurationInMs = 111;
        final long remainderInMs = SECONDS.toMillis(1) - nonSecondDurationInMs;

        final String firstMessage = sendTestRequest(nonSecondDurationInMs);
        final String secondMessage = sendTestRequest(remainderInMs);

        assertThat(firstMessage, containsString(":00.111\001"));
        assertThat(secondMessage, containsString(":01\001"));
    }

    private String sendTestRequest(final long nonSecondDurationInMs)
    {
        testRequest.reset();
        testRequest.testReqID("testReqID");
        fakeClock.advanceMilliSeconds(nonSecondDurationInMs);
        session().send(testRequest);
        return getSentMessage();
    }

    private String getSentMessage()
    {
        final MutableAsciiBuffer buffer = (MutableAsciiBuffer)this.bufferCaptor.getValue();
        return buffer.getAscii(offsetCaptor.getValue(), lengthCaptor.getValue());
    }

    private void verifySetupSession()
    {
        verify(mockProxy, atLeastOnce()).setupSession(anyLong(), any());
    }

    private void verifySetsSequenceNumbersToTwo(final int sequenceIndex)
    {
        verifySetsSentSequenceNumbersToTwo(sequenceIndex);
        assertEquals(1, session().lastReceivedMsgSeqNum());
    }

    private void verifySetsSentSequenceNumbersToTwo(final int sequenceIndex)
    {
        verify(mockProxy).logon(eq(HEARTBEAT_INTERVAL), eq(1), any(), any(), eq(true), eq(sequenceIndex));
        assertEquals(1, session().lastSentMsgSeqNum());
        verifyNoFurtherMessages();
    }

    private void sequenceNumbersAreThreeAndActive()
    {
        givenActive();
        sequenceNumbersAreThree();
    }

    private void sequenceNumbersAreThree()
    {
        session().lastReceivedMsgSeqNum(3).lastSentMsgSeqNum(3);
    }

    private void incorrectBeginStringLogout(final int times)
    {
        verify(mockProxy, times(times)).incorrectBeginStringLogout(1, SEQUENCE_INDEX);
    }

    private void heartbeatSentAfterInterval(
        final int heartbeatInterval,
        final int recvMsgSeqNo,
        final boolean backPressured)
    {
        if (backPressured)
        {
            when(mockProxy.heartbeat(anyInt(), eq(SEQUENCE_INDEX))).thenReturn(BACK_PRESSURED, POSITION);
        }

        final int sentMsgSeqNo = nextMsgSeqNum();

        fakeClock.advanceSeconds(heartbeatInterval + DEFAULT_REASONABLE_TRANSMISSION_TIME_IN_S);

        onMessage(recvMsgSeqNo);

        fakeClock.advanceSeconds(1);

        poll();

        if (backPressured)
        {
            poll();
        }

        verify(mockProxy, retry(backPressured)).heartbeat(sentMsgSeqNo, SEQUENCE_INDEX);
        reset(mockProxy);
    }

    private VerificationMode retry(final boolean backPressured)
    {
        return times(backPressured ? 2 : 1);
    }

    public void verifyDisconnect(final VerificationMode times)
    {
        verify(mockProxy, times).requestDisconnect(eq(CONNECTION_ID), any());
        assertState(DISCONNECTED);
    }

    private void backPressureDisconnect()
    {
        when(mockProxy.requestDisconnect(eq(CONNECTION_ID), any())).thenReturn(BACK_PRESSURED, POSITION);
    }

    protected void givenActive()
    {
        session().state(ACTIVE);
    }

    public void verifyLogout(final int msgSeqNo, final VerificationMode times)
    {
        verify(mockProxy, times).logout(msgSeqNo, SEQUENCE_INDEX);
    }

    public void assertState(final SessionState state)
    {
        assertEquals(state, session().state());
    }

    public Action onLogon(final int msgSeqNo)
    {
        return onLogon(HEARTBEAT_INTERVAL, msgSeqNo, false);
    }

    private Action onLogon(final int heartbeatInterval, final int msgSeqNo, final boolean resetSeqNumFlag)
    {
        final String username = null;
        final String password = null;
        return session().onLogon(
            heartbeatInterval,
            msgSeqNo,
            SESSION_ID,
            SESSION_KEY,
            fakeClock.time(),
            UNKNOWN,
            username,
            password,
            false,
            resetSeqNumFlag);
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
        assertEquals(CONTINUE, session().onLogout(1, sendingTime(), UNKNOWN, false));
    }

    protected void verifySendingTimeProblem()
    {
        verify(mockProxy).reject(
            2, 2, 52, MSG_TYPE_BYTES, MSG_TYPE_BYTES.length, SENDINGTIME_ACCURACY_PROBLEM, SEQUENCE_INDEX);
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
        fakeClock.advanceSeconds(SESSION_TIMEOUT * 2);
    }

    protected void verifyConnected()
    {
        verify(mockProxy, never()).requestDisconnect(CONNECTION_ID, APPLICATION_DISCONNECT);
    }

    protected void verifyCanRoundtripTestMessage()
    {
        final char[] testReqId = "Hello".toCharArray();
        final int testReqIdLength = 5;

        session().onTestRequest(4, testReqId, testReqIdLength, sendingTime(), UNKNOWN, false);
        verify(mockProxy).heartbeat(eq(testReqId), eq(testReqIdLength), anyInt(), eq(SEQUENCE_INDEX));
        verifyConnected();
    }

    protected void poll()
    {
        session().poll(fakeClock.time());
    }

    protected abstract Session session();

    protected abstract void readyForLogon();

    void verifyNoFurtherMessages()
    {
        verifyNoMoreInteractions(mockProxy);
    }

    private void assertSequenceIndexIs(final int sequenceIndex)
    {
        assertEquals(sequenceIndex, session().sequenceIndex());
    }
}
