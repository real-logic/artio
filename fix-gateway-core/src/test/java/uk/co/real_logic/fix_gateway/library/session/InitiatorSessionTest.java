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
package uk.co.real_logic.fix_gateway.library.session;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.DEFAULT_SESSION_BUFFER_SIZE;
import static uk.co.real_logic.fix_gateway.messages.SessionState.*;

public class InitiatorSessionTest extends AbstractSessionTest
{
    private InitiatorSession session = new InitiatorSession(HEARTBEAT_INTERVAL, CONNECTION_ID, fakeClock, mockProxy,
        mockPublication, null, BEGIN_STRING, SENDING_TIME_WINDOW,
        mockReceivedMsgSeqNo, mockSentMsgSeqNo, null, null, LIBRARY_ID, DEFAULT_SESSION_BUFFER_SIZE, 1, CONNECTED);

    @Test
    public void shouldInitiallyBeConnected()
    {
        assertEquals(CONNECTED, session.state());
    }

    @Test
    public void shouldActivateUponLogonResponse()
    {
        session.state(SENT_LOGON);

        onLogon(1);

        assertState(ACTIVE);
        verify(mockProxy).setupSession(SESSION_ID, SESSION_KEY);
        verifyNoFurtherMessages();
    }

    @Test
    public void shouldAttemptLogonWhenConnected()
    {
        session.id(SESSION_ID);
        session.poll(0);

        verifyLogon();

        assertEquals(1, session.lastSentMsgSeqNum());
    }

    @Test
    public void shouldAttemptLogonOnlyOnce()
    {
        session.id(SESSION_ID);
        session.poll(0);

        session.poll(10);

        session.poll(20);

        verifyLogon();
    }

    @Test
    public void shouldNotifyGatewayWhenLoggedIn()
    {
        session.state(SENT_LOGON);

        onLogon(1);

        verifySavesLogonMessage();
    }

    @Test
    public void shouldNotifyGatewayWhenLoggedInOnce()
    {
        session.state(SENT_LOGON);

        onLogon(1);

        onLogon(2);

        verifySavesLogonMessage();
    }

    @Test
    public void shouldDisconnectIfMissingSequenceNumber()
    {
        shouldDisconnectIfMissingSequenceNumber(1);
    }

    private void verifySavesLogonMessage()
    {
        verify(mockPublication, times(1)).saveLogon(LIBRARY_ID, CONNECTION_ID, SESSION_ID);
    }

    private void verifyLogon()
    {
        verify(mockProxy, times(1)).logon(HEARTBEAT_INTERVAL, 1, null, null);
    }

    protected void readyForLogon()
    {
        session.state(SENT_LOGON);
    }

    protected Session session()
    {
        return session;
    }
}
