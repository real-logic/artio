/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.library.session;

import org.junit.Test;
import uk.co.real_logic.fix_gateway.decoder.SequenceResetDecoder;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.SessionRejectReason.*;
import static uk.co.real_logic.fix_gateway.decoder.Constants.NEW_SEQ_NO;
import static uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.fix_gateway.library.SessionConfiguration.DEFAULT_SESSION_BUFFER_SIZE;
import static uk.co.real_logic.fix_gateway.library.session.Session.TEST_REQ_ID;
import static uk.co.real_logic.fix_gateway.library.session.Session.UNKNOWN;
import static uk.co.real_logic.fix_gateway.library.session.SessionState.*;

public class SessionTest extends AbstractSessionTest
{
    public static final long TWO_MINUTES = MINUTES.toMillis(2);

    private Session session = new Session(
        HEARTBEAT_INTERVAL,
        CONNECTION_ID,
        fakeClock,
        ACTIVE,
        mockProxy,
        mockPublication,
        null,
        BEGIN_STRING,
        SENDING_TIME_WINDOW,
        mockReceivedMsgSeqNo,
        mockSentMsgSeqNo,
        LIBRARY_ID,
        DEFAULT_SESSION_BUFFER_SIZE,
        1);

    @Test
    public void shouldReplyToValidLogout()
    {
        session.state(ACTIVE);

        onLogout();

        verifyLogout();
        verifyDisconnect();
    }

    @Test
    public void shouldDisconnectUponLogoutAcknowledgement()
    {
        session.state(AWAITING_LOGOUT);

        onLogout();

        verifyDisconnect();
    }

    @Test
    public void shouldReplyToTestRequestsWithAHeartbeat()
    {
        final char[] testReqId = "ABC".toCharArray();
        final int testReqIdLength = testReqId.length;

        session.id(SESSION_ID);

        session.onTestRequest(1, testReqId, testReqIdLength, sendingTime(), UNKNOWN, false);

        verify(mockProxy).heartbeat(testReqId, testReqIdLength, 1);
    }

    @Test
    public void shouldResendRequestForUnexpectedGapFill()
    {
        session.id(SESSION_ID);

        session.onSequenceReset(3, 4, true, false);
        onMessage(3);

        verify(mockProxy).resendRequest(1, 1, 0);
    }

    @Test
    public void shouldIgnoreDuplicateGapFill()
    {
        session.lastReceivedMsgSeqNum(2);

        session.onSequenceReset(1, 4, false, true);

        verifyNoFurtherMessages();
    }

    @Test
    public void shouldLogoutOnInvalidGapFill()
    {
        session.lastReceivedMsgSeqNum(2);

        session.onSequenceReset(1, 4, true, false);

        verify(mockProxy).lowSequenceNumberLogout(anyInt(), eq(3), eq(1));
        verifyDisconnect();
    }

    @Test
    public void shouldUpdateSequenceNumberOnValidGapFill()
    {
        session.onSequenceReset(1, 4, true, false);

        assertEquals(4, session.expectedReceivedSeqNum());
        verifyNoFurtherMessages();
        verifyConnected();

        verifyCanRoundtripTestMessage();
    }

    @Test
    public void shouldIgnoreMsgSeqNumWithoutGapFillFlag()
    {
        session.onSequenceReset(0, 4, false, false);

        assertEquals(4, session.expectedReceivedSeqNum());
        verifyNoFurtherMessages();
        verifyConnected();

        verifyCanRoundtripTestMessage();
    }

    @Test
    public void shouldUpdateSequenceNumberOnSequenceReset()
    {
        session.onSequenceReset(4, 4, false, false);

        assertEquals(4, session.expectedReceivedSeqNum());
        verifyNoFurtherMessages();
    }

    @Test
    public void shouldAcceptUnnecessarySequenceReset()
    {
        session.lastReceivedMsgSeqNum(3);

        session.onSequenceReset(4, 4, false, false);

        assertEquals(4, session.expectedReceivedSeqNum());
        verifyNoFurtherMessages();
    }

    @Test
    public void shouldRejectLowSequenceReset()
    {
        session.lastReceivedMsgSeqNum(3);

        session.onSequenceReset(2, 1, false, false);

        assertEquals(4, session.expectedReceivedSeqNum());
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
        session.state(ACTIVE);
        session.lastReceivedMsgSeqNum(9);

        onMessage(10);

        twoHeartBeatIntervalsPass();

        poll();

        verify(mockProxy).testRequest(anyInt(), eq(TEST_REQ_ID));
        assertEquals(AWAITING_RESEND, session.state());
    }

    @Test
    public void shouldLogoutAndDisconnectUponTimeout()
    {
        shouldSendTestRequestUponTimeout();

        twoHeartBeatIntervalsPass();

        poll();

        verifyDisconnect();
    }

    @Test
    public void shouldSuppressTimeoutWhenMessageReceived()
    {
        session.state(ACTIVE);
        session.lastReceivedMsgSeqNum(9);

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
        session.state(ACTIVE);
        session.id(SESSION_ID);

        onMessage(3);

        verify(mockProxy).resendRequest(1, 1, 0);
        assertState(AWAITING_RESEND);
    }

    @Test
    public void shouldLogoutIfNegativeHeartbeatInterval()
    {
        final int heartbeatInterval = -1;

        session().onLogon(heartbeatInterval, 0, SESSION_ID, SESSION_KEY, fakeClock.time(), UNKNOWN, null, null, false);

        verify(mockProxy).negativeHeartbeatLogout(1);
    }

    @Test
    public void shouldSendHeartbeatAfterInterval()
    {
        onLogon(1);

        heartbeatSentAfterInterval(1, 2);
    }

    @Test
    public void shouldSendHeartbeatAfterLogonSpecifiedInterval()
    {
        final int heartbeatInterval = 1;
        session().onLogon(heartbeatInterval, 1, SESSION_ID, null, fakeClock.time(), UNKNOWN, null, null, false);

        heartbeatSentAfterInterval(heartbeatInterval, 2);
    }

    @Test
    public void shouldSendHeartbeatsAfterIntervalRepeatedly()
    {
        shouldSendHeartbeatAfterInterval();

        heartbeatSentAfterInterval(2, 3);

        heartbeatSentAfterInterval(3, 4);
    }

    @Test
    public void shouldDisconnectIfBeginStringIsInvalidAtLogon()
    {
        onBeginString(true);
        verifyDisconnect();
    }

    @Test
    public void shouldDisconnectIfBeginStringIsInvalid()
    {
        onBeginString(false);
        verify(mockProxy).incorrectBeginStringLogout(anyInt());
        verifyDisconnect();
    }

    private void onBeginString(final boolean isLogon)
    {
        final char[] beginString = "FIX.3.9 ".toCharArray();
        final boolean valid = session.onBeginString(beginString, beginString.length - 1, isLogon);
        assertFalse(valid);
    }

    @Test
    public void shouldDisconnectIfMissingSequenceNumber()
    {
        onLogon(1);

        onMessage(MISSING_INT);

        verify(mockProxy).receivedMessageWithoutSequenceNumber(1);
        verifyDisconnect();
    }

    @Test
    public void shouldValidateSendingTimeNotTooEarly()
    {
        onLogon(1);

        messageWithWeirdTime(sendingTime() - TWO_MINUTES);

        verifySendingTimeProblem();
        verifyLogout();
        verifyDisconnect();
    }

    @Test
    public void shouldValidateSendingTimeNotTooLate()
    {
        onLogon(1);

        messageWithWeirdTime(sendingTime() + TWO_MINUTES);

        verifySendingTimeProblem();
        verifyLogout();
        verifyDisconnect();
    }

    @Test
    public void shouldValidateOriginalSendingTimeBeforeSendingTime()
    {
        final long sendingTime = sendingTime();
        final long origSendingTime = sendingTime + 10;

        onLogon(1);

        onMessage(2);

        session.onMessage(2, MSG_TYPE_BYTES, sendingTime, origSendingTime, true);

        verifySendingTimeProblem();
    }

    @Test
    public void shouldValidateOriginalSendingTimeExistsIfPossDupFlagIsSet()
    {
        onLogon(1);

        onMessage(2);

        session.onMessage(2, MSG_TYPE_BYTES, sendingTime(), UNKNOWN, true);

        verify(mockProxy).reject(1, 2, MSG_TYPE_BYTES, MSG_TYPE_BYTES.length, REQUIRED_TAG_MISSING);
    }

    @Test
    public void shouldNotifyClientUponSequenceReset()
    {
        final int newSeqNo = 10;

        onLogon(1);

        assertEquals(0, session.lastSentMsgSeqNum());

        session.sequenceReset(newSeqNo);

        verify(mockProxy).sequenceReset(anyInt(), eq(newSeqNo));
        assertEquals(newSeqNo - 1, session.lastSentMsgSeqNum());
    }

    private void poll()
    {
        session.poll(fakeClock.time());
    }

    private void twoHeartBeatIntervalsPass()
    {
        fakeClock.advanceSeconds(HEARTBEAT_INTERVAL * 2);
    }

    private void verifyConnected()
    {
        verify(mockProxy, never()).requestDisconnect(CONNECTION_ID);
    }

    private void verifyCanRoundtripTestMessage()
    {
        final char[] testReqId = "Hello".toCharArray();
        final int testReqIdLength = 5;

        session.onTestRequest(4, testReqId, testReqIdLength, sendingTime(), UNKNOWN, false);
        verify(mockProxy).heartbeat(eq(testReqId), eq(testReqIdLength), anyInt());
        verifyConnected();
    }

    private void heartbeatSentAfterInterval(final int heartbeatInterval,
                                            final int msgSeqNo)
    {
        fakeClock.advanceSeconds(heartbeatInterval);

        onMessage(msgSeqNo);

        fakeClock.advanceSeconds(1);

        poll();

        verify(mockProxy).heartbeat(anyInt());
        reset(mockProxy);
    }

    private void onLogout()
    {
        session.onLogout(1, sendingTime(), UNKNOWN, false);
    }

    private void verifySendingTimeProblem()
    {
        verify(mockProxy).reject(1, 2, MSG_TYPE_BYTES, MSG_TYPE_BYTES.length, SENDINGTIME_ACCURACY_PROBLEM);
    }

    private void messageWithWeirdTime(final long sendingTime)
    {
        session().onMessage(2, MSG_TYPE_BYTES, sendingTime, UNKNOWN, false);
    }

    protected Session session()
    {
        return session;
    }
}
