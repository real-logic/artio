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
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.fix_gateway.framer.FakeMilliClock;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.session.SessionState.*;

public abstract class AbstractSessionTest
{
    public static final long SENDING_TIME_WINDOW = 2000;
    public static final char[] BEGIN_STRING = "FIX.4.4".toCharArray();
    public static final long CONNECTION_ID = 3L;
    public static final long SESSION_ID = 2L;
    public static final int HEARTBEAT_INTERVAL = 2;
    public static final Object SESSION_KEY = new Object();

    protected SessionProxy mockProxy = mock(SessionProxy.class);
    protected GatewayPublication mockPublication = mock(GatewayPublication.class);
    protected FakeMilliClock fakeClock = new FakeMilliClock();
    protected SessionIds mockSessionIds = mock(SessionIds.class);
    protected AtomicCounter mockReceivedMsgSeqNo = mock(AtomicCounter.class);
    protected AtomicCounter mockSentMsgSeqNo = mock(AtomicCounter.class);

    public void verifyNoFurtherMessages()
    {
        verifyNoMoreInteractions(mockProxy);
    }

    @Test
    public void shouldLogoutOnLowSequenceNumber()
    {
        session().state(ACTIVE);
        session().lastReceivedMsgSeqNum(2);

        session().onMessage(1);
        verify(mockProxy).lowSequenceNumberLogout(1, 3, 1);
        verifyDisconnect();
    }

    public void verifyDisconnect()
    {
        verify(mockProxy).disconnect(CONNECTION_ID);
        assertState(DISCONNECTED);
    }

    public void verifyLogoutStarted()
    {
        verifyLogout();
        awaitingLogout();
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
       session().onLogon(HEARTBEAT_INTERVAL, msgSeqNo, SESSION_ID, SESSION_KEY, fakeClock.time());
    }

    protected abstract Session session();
}
