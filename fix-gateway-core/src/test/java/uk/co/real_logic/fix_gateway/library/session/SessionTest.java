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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.SessionRejectReason.VALUE_IS_INCORRECT;
import static uk.co.real_logic.fix_gateway.decoder.Constants.NEW_SEQ_NO;
import static uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.fix_gateway.library.session.Session.TEST_REQ_ID;
import static uk.co.real_logic.fix_gateway.library.session.SessionState.*;

public class SessionTest extends AbstractSessionTest
{
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
        mockSentMsgSeqNo);

    @Test
    public void shouldReplyToValidLogout()
    {
        session.state(ACTIVE);

        session.onLogout(1, false);

        verifyLogout();
        verifyDisconnect();
    }

    @Test
    public void shouldDisconnectUponLogoutAcknowledgement()
    {
        session.state(AWAITING_LOGOUT);

        session.onLogout(1, false);

        verifyDisconnect();
    }

    @Test
    public void shouldReplyToTestRequestsWithAHeartbeat()
    {
        final char[] testReqId = "ABC".toCharArray();
        final int testReqIdLength = testReqId.length;

        session.id(SESSION_ID);

        session.onTestRequest(testReqId, testReqIdLength, 1, false);

        verify(mockProxy).heartbeat(testReqId, testReqIdLength, 1);
    }

    @Test
    public void shouldResendRequestForUnexpectedGapFill()
    {
        session.id(SESSION_ID);

        session.onSequenceReset(3, 4, true, false);
        session.onMessage(3, false);

        verify(mockProxy).resendRequest(5, 1, 2);
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
        verify(mockProxy).reject(1, 2, NEW_SEQ_NO, SequenceResetDecoder.MESSAGE_TYPE_BYTES, VALUE_IS_INCORRECT);
    }

    @Test
    public void shouldSendTestRequestUponTimeout()
    {
        session.state(ACTIVE);
        session.lastReceivedMsgSeqNum(9);

        session.onMessage(10, false);

        twoHeartBeatIntervalsPass();

        poll();

        verify(mockProxy).testRequest(2, TEST_REQ_ID);
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

        session.onMessage(10, false);

        fakeClock.advanceSeconds(1);

        poll();
        session.onMessage(11, false);

        fakeClock.advanceSeconds(1);

        poll();

        verifyConnected();
    }

    @Test
    public void shouldRequestResendIfHighSeqNo()
    {
        session.state(ACTIVE);
        session.id(SESSION_ID);

        session.onMessage(3, false);

        verify(mockProxy).resendRequest(1, 1, 2);
        assertState(AWAITING_RESEND);
    }

    @Test
    public void shouldLogoutIfNegativeHeartbeatInterval()
    {
        final int heartbeatInterval = -1;

        session().onLogon(heartbeatInterval, 0, SESSION_ID, SESSION_KEY, fakeClock.time(), false);

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
        session().onLogon(heartbeatInterval, 1, SESSION_ID, null, fakeClock.time(), false);

        heartbeatSentAfterInterval(heartbeatInterval, 1, 2);
    }

    @Test
    public void shouldSendHeartbeatsAfterIntervalRepeatedly()
    {
        onLogon(1);

        heartbeatSentAfterInterval(1, 2);

        heartbeatSentAfterInterval(2, 3);

        heartbeatSentAfterInterval(3, 4);
    }

    @Test
    public void shouldDisconnectIfBeginStringIsInvalid()
    {
        final char[] beginString = "FIX.3.9 ".toCharArray();

        final boolean valid = session.onBeginString(beginString, beginString.length - 1);

        assertFalse(valid);
        verifyDisconnect();
    }

    @Test
    public void shouldDisconnectIfMissingSequenceNumber()
    {
        onLogon(1);

        session.onMessage(MISSING_INT, false);

        verify(mockProxy).receivedMessageWithoutSequenceNumber(1);
        verifyDisconnect();
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

    private void heartbeatSentAfterInterval(final int heartbeatMsgSeqNo, final int msgSeqNo)
    {
        heartbeatSentAfterInterval(HEARTBEAT_INTERVAL, heartbeatMsgSeqNo, msgSeqNo);
    }

    private void verifyCanRoundtripTestMessage()
    {
        final char[] testReqId = "Hello".toCharArray();
        final int testReqIdLength = 5;

        session.onTestRequest(testReqId, testReqIdLength, 4, false);
        verify(mockProxy).heartbeat(eq(testReqId), eq(testReqIdLength), anyInt());
        verifyConnected();
    }

    private void heartbeatSentAfterInterval(final int heartbeatInterval,
                                            final int heartbeatMsgSeqNo,
                                            final int msgSeqNo)
    {
        fakeClock.advanceSeconds(heartbeatInterval);

        session.onMessage(msgSeqNo, false);

        fakeClock.advanceSeconds(1);

        poll();

        verify(mockProxy).heartbeat(heartbeatMsgSeqNo);
        reset(mockProxy);
    }

    protected Session session()
    {
        return session;
    }
}
