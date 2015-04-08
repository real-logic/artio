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
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.session.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.session.SessionState.AWAITING_RESEND;

public class SessionTest extends AbstractSessionTest
{
    private Session session = new Session(
        HEARTBEAT_INTERVAL, CONNECTION_ID, fakeClock, ACTIVE, mockProxy, mockPublication, null);

    @Test
    public void shouldReplyToValidLogout()
    {
        session.state(ACTIVE);

        session.onLogout(1, SESSION_ID);

        verify(mockProxy).logout(1, SESSION_ID);
        verifyDisconnect();
    }

    @Test
    public void shouldReplyToTestRequestsWithAHeartbeat()
    {
        session.id(SESSION_ID);

        // TODO: figure out the correct String type here
        session.onTestRequest("ABC");

        verify(mockProxy).heartbeat("ABC", SESSION_ID);
    }

    @Test
    public void shouldResendRequestForUnexpectedGapFill()
    {
        session.id(SESSION_ID);

        session.onSequenceReset(3, 4, false);
        session.onMessage(3);

        verify(mockProxy).resendRequest(5, 1, 2, SESSION_ID);
    }

    @Test
    public void shouldIgnoreDuplicateGapFill()
    {
        session.lastReceivedMsgSeqNum(2);

        session.onSequenceReset(1, 4, true);

        verifyNoFurtherMessages();
    }

    @Test
    public void shouldDisconnectOnInvalidGapFill()
    {
        session.lastReceivedMsgSeqNum(2);

        session.onSequenceReset(1, 4, false);

        verifyDisconnect();
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
        verify(mockProxy).reject(4, 2, Session.UNKNOWN_ID);
    }

    // NB: differs from the spec to disconnect, rather than test request.
    @Test
    public void shouldDisconnectUponTimeout()
    {
        session.state(ACTIVE);
        session.lastReceivedMsgSeqNum(9);

        session.onMessage(10);

        fakeClock.advanceSeconds(HEARTBEAT_INTERVAL * 2);

        session.poll(fakeClock.time());

        verifyDisconnect();
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

        verify(mockProxy).resendRequest(1, 1, 2, SESSION_ID);
        assertState(AWAITING_RESEND);
    }

    @Test
    public void shouldLogoutIfLowSeqNo()
    {
        session.state(ACTIVE);
        session.lastReceivedMsgSeqNum(2);

        session.onMessage(1);
        verifyDisconnect();
    }

    @Test
    public void shouldSendHeartbeatAfterInterval()
    {
        onLogon(0);

        heartbeatSentAfterInterval();
    }

    @Test
    public void shouldSendHeartbeatAfterLogonSpecifiedInterval()
    {
        session().onLogon(1, 0, SESSION_ID, null);
        session().onMessage(0);

        heartbeatSentAfterInterval(1);
    }

    @Test
    public void shouldSendHeartbeatsAfterIntervalRepeatedly()
    {
        onLogon(0);

        heartbeatSentAfterInterval();

        heartbeatSentAfterInterval();

        heartbeatSentAfterInterval();
    }

    private void heartbeatSentAfterInterval()
    {
        heartbeatSentAfterInterval(HEARTBEAT_INTERVAL);
    }

    private void heartbeatSentAfterInterval(final int heartbeatInterval)
    {
        fakeClock.advanceSeconds(heartbeatInterval);

        session.poll(fakeClock.time());

        verify(mockProxy).heartbeat(null, SESSION_ID);
        reset(mockProxy);
    }


    protected Session session()
    {
        return session;
    }
}
