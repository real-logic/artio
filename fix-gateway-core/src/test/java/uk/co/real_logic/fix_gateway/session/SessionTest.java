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
package uk.co.real_logic.fix_gateway.session;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.fix_gateway.session.SessionState.*;

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
        mockSessionIds,
        mockReceivedMsgSeqNo,
        mockSentMsgSeqNo);

    @Test
    public void shouldReplyToValidLogout()
    {
        session.state(ACTIVE);

        session.onLogout(1);

        verifyLogout();
        verifyDisconnect();
    }

    @Test
    public void shouldDisconnectUponLogoutAcknowledgement()
    {
        session.state(AWAITING_LOGOUT);

        session.onLogout(1);

        verifyDisconnect();
    }

    @Test
    public void shouldReplyToTestRequestsWithAHeartbeat()
    {
        final char[] testReqId = "ABC".toCharArray();
        final int testReqIdLength = testReqId.length;

        session.id(SESSION_ID);

        session.onTestRequest(testReqId, testReqIdLength, 1);

        verify(mockProxy).heartbeat(testReqId, testReqIdLength, 1);
    }

    @Test
    public void shouldResendRequestForUnexpectedGapFill()
    {
        session.id(SESSION_ID);

        session.onSequenceReset(3, 4, false);
        session.onMessage(3);

        verify(mockProxy).resendRequest(5, 1, 2);
    }

    @Test
    public void shouldIgnoreDuplicateGapFill()
    {
        session.lastReceivedMsgSeqNum(2);

        session.onSequenceReset(1, 4, true);

        verifyNoFurtherMessages();
    }

    @Test
    public void shouldLogoutOnInvalidGapFill()
    {
        session.lastReceivedMsgSeqNum(2);

        session.onSequenceReset(1, 4, false);

        verifyLogoutStarted();
    }

    @Test
    public void shouldUpdateSequenceNumberOnValidGapFill()
    {
        session.onSequenceReset(1, 4, false);

        assertEquals(4, session.expectedReceivedSeqNum());
        verifyNoFurtherMessages();
    }

    @Test
    public void shouldUpdateSequenceNumberOnSequenceReset()
    {
        session.onSequenceReset(4, 4, false);

        assertEquals(4, session.expectedReceivedSeqNum());
        verifyNoFurtherMessages();
    }

    @Test
    public void shouldAcceptUnnecessarySequenceReset()
    {
        session.lastReceivedMsgSeqNum(3);

        session.onSequenceReset(4, 4, false);

        assertEquals(4, session.expectedReceivedSeqNum());
        verifyNoFurtherMessages();
    }

    @Test
    public void shouldRejectLowSequenceReset()
    {
        session.lastReceivedMsgSeqNum(3);

        session.onSequenceReset(2, 1, false);

        assertEquals(4, session.expectedReceivedSeqNum());
        verify(mockProxy).reject(4, 2);
    }

    // NB: differs from the spec to disconnect, rather than test request.
    @Test
    public void shouldLogoutUponTimeout()
    {
        session.state(ACTIVE);
        session.lastReceivedMsgSeqNum(9);

        session.onMessage(10);

        fakeClock.advanceSeconds(HEARTBEAT_INTERVAL * 2);

        session.poll(fakeClock.time());

        verifyLogoutStarted();
    }

    @Test
    public void shouldSuppressTimeout()
    {
        session.state(ACTIVE);
        session.lastReceivedMsgSeqNum(9);

        session.onMessage(10);

        fakeClock.advanceSeconds(1);

        session.poll(fakeClock.time());
        session.onMessage(11);

        fakeClock.advanceSeconds(1);

        session.poll(fakeClock.time());

        verifyConnected();
    }

    private void verifyConnected()
    {
        verify(mockProxy, never()).disconnect(CONNECTION_ID);
    }

    @Test
    public void shouldRequestResendIfHighSeqNo()
    {
        session.state(ACTIVE);
        session.id(SESSION_ID);

        session.onMessage(3);

        verify(mockProxy).resendRequest(1, 1, 2);
        assertState(AWAITING_RESEND);
    }

    @Test
    public void shouldLogoutIfNegativeHeartbeatInterval()
    {
        final int heartbeatInterval = -1;

        session().onLogon(heartbeatInterval, 0, SESSION_ID, SESSION_KEY, fakeClock.time());

        verify(mockProxy).negativeHeartbeatLogout(1);
    }

    @Test
    public void shouldSendHeartbeatAfterInterval()
    {
        onLogon(0);

        heartbeatSentAfterInterval(3);
    }

    @Test
    public void shouldSendHeartbeatAfterLogonSpecifiedInterval()
    {
        session().onLogon(1, 0, SESSION_ID, null, fakeClock.time());
        session().onMessage(0);

        heartbeatSentAfterInterval(1, 4);
    }

    @Test
    public void shouldSendHeartbeatsAfterIntervalRepeatedly()
    {
        onLogon(0);

        heartbeatSentAfterInterval(3);

        heartbeatSentAfterInterval(4);

        heartbeatSentAfterInterval(6);
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

        session.onMessage(MISSING_INT);

        verify(mockProxy).receivedMessageWithoutSequenceNumber(1);
        verifyDisconnect();
    }

    private void heartbeatSentAfterInterval(final int msgSeqNo)
    {
        heartbeatSentAfterInterval(HEARTBEAT_INTERVAL, msgSeqNo);
    }

    private void heartbeatSentAfterInterval(final int heartbeatInterval, final int msgSeqNo)
    {
        fakeClock.advanceSeconds(heartbeatInterval);

        session.poll(fakeClock.time());

        verify(mockProxy).heartbeat(msgSeqNo);
        reset(mockProxy);
    }

    protected Session session()
    {
        return session;
    }
}
