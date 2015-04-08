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

import uk.co.real_logic.fix_gateway.framer.FakeMilliClock;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.session.SessionState.DISCONNECTED;

public abstract class AbstractSessionTest
{
    public static final long CONNECTION_ID = 3L;
    public static final long SESSION_ID = 2L;
    public static final int HEARTBEAT_INTERVAL = 2;

    protected SessionProxy mockProxy = mock(SessionProxy.class);
    protected GatewayPublication mockPublication = mock(GatewayPublication.class);
    protected FakeMilliClock fakeClock = new FakeMilliClock();

    public void verifyNoFurtherMessages()
    {
        verifyNoMoreInteractions(mockProxy);
    }

    public void verifyDisconnect()
    {
        verify(mockProxy).disconnect(CONNECTION_ID);
        assertState(DISCONNECTED);
    }

    public void assertState(final SessionState state)
    {
        assertEquals(state, session().state());
    }

    public void onLogon(final int msgSeqNo)
    {
        session().onLogon(HEARTBEAT_INTERVAL, msgSeqNo, SESSION_ID, null);
    }

    protected abstract Session session();
}
