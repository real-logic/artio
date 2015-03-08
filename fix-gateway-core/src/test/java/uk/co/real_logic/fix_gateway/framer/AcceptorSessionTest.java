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
package uk.co.real_logic.fix_gateway.framer;

import org.junit.Test;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static uk.co.real_logic.fix_gateway.framer.SessionState.*;

public class AcceptorSessionTest
{
    private static final long CONNECTION_ID = 3L;
    private static final long SESSION_ID = 2L;
    private static final long HEARTBEAT_INTERVAL = 2L;

    private SessionProxy mockProxy = mock(SessionProxy.class);

    private long currentTime = 0;
    private MilliClock mockClock = () -> currentTime;

    private AcceptorSession session = new AcceptorSession(HEARTBEAT_INTERVAL, CONNECTION_ID, mockClock, mockProxy);

    @Test
    public void shouldInitiallyBeConnected()
    {
        assertEquals(CONNECTED, session.state());
    }

    @Test
    public void shouldBeActivatedBySuccessfulLogin()
    {
        onLogon(1);

        verify(mockProxy).logon(HEARTBEAT_INTERVAL, 2, SESSION_ID);
        assertState(ACTIVE);
    }

    @Test
    public void shouldRequestResendIfHighSeqNoLogon()
    {
        onLogon(3);

        verify(mockProxy).resendRequest(1, 2);
        assertState(AWAITING_RESEND);
    }

    @Test
    public void shouldLogoutIfLowSeqNoLogon()
    {
        session.lastMsgSeqNum(2);

        onLogon(1);
        verifyDisconnect();
    }

    @Test
    public void shouldReplyToValidLogout()
    {
        session.state(ACTIVE);

        session.onLogout(1, SESSION_ID);

        verify(mockProxy).logout(2, SESSION_ID);
        verifyDisconnect();
    }

    @Test
    public void shouldDisconnectIfFirstMessageNotALogon()
    {
        session.onMessage(1);

        verifyDisconnect();
    }

    @Test
    public void shouldReplyToTestRequestsWithAHeartbeat()
    {
        // TODO: figure out the correct String type here
        session.onTestRequest("ABC");

        verify(mockProxy).heartbeat("ABC");
    }

    @Test
    public void shouldResendRequestForUnexpectedGapFill()
    {
        session.onSequenceReset(3, 4, false);

        verify(mockProxy).resendRequest(1, 2);
    }

    @Test
    public void shouldIgnoreDuplicateGapFill()
    {
        session.lastMsgSeqNum(2);

        session.onSequenceReset(1, 4, true);

        verifyNoMessages();
    }

    @Test
    public void shouldDisconnectOnInvalidGapFill()
    {
        session.lastMsgSeqNum(2);

        session.onSequenceReset(1, 4, false);

        verifyDisconnect();
    }

    @Test
    public void shouldUpdateSequenceNumberOnValidGapFill()
    {
        session.onSequenceReset(1, 4, false);

        assertEquals(4, session.expectedSeqNo());
        verifyNoMessages();
    }

    @Test
    public void shouldUpdateSequenceNumberOnSequenceReset()
    {
        session.onSequenceReset(4, 4, false);

        assertEquals(4, session.expectedSeqNo());
        verifyNoMessages();
    }

    @Test
    public void shouldAcceptUnnecessarySequenceReset()
    {
        session.lastMsgSeqNum(3);

        session.onSequenceReset(4, 4, false);

        assertEquals(4, session.expectedSeqNo());
        verifyNoMessages();
    }

    @Test
    public void shouldRejectLowSequenceReset()
    {
        session.lastMsgSeqNum(3);

        session.onSequenceReset(2, 1, false);

        assertEquals(4, session.expectedSeqNo());
        verify(mockProxy).reject(2);
    }

    // NB: differs from the spec to disconnect, rather than test request.
    @Test
    public void shouldDisconnectUponTimeout()
    {
        session.state(ACTIVE);
        session.lastMsgSeqNum(9);

        session.onMessage(10);

        currentTime += HEARTBEAT_INTERVAL * 2;

        session.poll();

        verifyDisconnect();
    }

    @Test
    public void shouldSuppressTimeout()
    {
        session.state(ACTIVE);
        session.lastMsgSeqNum(9);

        session.onMessage(10);

        currentTime += 1;

        session.poll();
        session.onMessage(11);

        currentTime += 1;

        session.poll();

        verifyNoMessages();
    }

    @Test
    public void shouldRequestResendIfHighSeqNo()
    {
        session.state(ACTIVE);
        session.onMessage(3);

        verify(mockProxy).resendRequest(1, 2);
        assertState(AWAITING_RESEND);
    }

    @Test
    public void shouldLogoutIfLowSeqNo()
    {
        session.state(ACTIVE);
        session.lastMsgSeqNum(2);

        session.onMessage(1);
        verifyDisconnect();
    }

    private void verifyNoMessages()
    {
        verifyNoMoreInteractions(mockProxy);
    }

    private void verifyDisconnect()
    {
        verify(mockProxy).disconnect(CONNECTION_ID);
        assertState(DISCONNECTED);
    }

    private void assertState(final SessionState state)
    {
        assertEquals(state, session.state());
    }

    private void onLogon(final int msgSeqNo)
    {
        session.onLogon(HEARTBEAT_INTERVAL, msgSeqNo, SESSION_ID);
        session.onMessage(msgSeqNo);
    }
}
