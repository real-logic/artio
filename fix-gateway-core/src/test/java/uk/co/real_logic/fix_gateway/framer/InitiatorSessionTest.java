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

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.fix_gateway.framer.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.framer.SessionState.CONNECTING;

public class InitiatorSessionTest extends AbstractSessionTest
{
    private InitiatorSession session = new InitiatorSession(HEARTBEAT_INTERVAL, CONNECTION_ID, mockClock, mockProxy);

    @Test
    public void shouldInitiallyBeConnecting()
    {
        assertEquals(CONNECTING, session.state());
    }

    @Test
    public void shouldActivateUponLogonResponse()
    {
        session.connected();

        onLogon(1);

        assertState(ACTIVE);
    }

    @Test
    public void shouldDisconnectIfLowSeqNo()
    {
        session.connected();
        session.lastMsgSeqNum(5);

        onLogon(1);

        verifyDisconnect();
    }

    protected Session session()
    {
        return session;
    }
}
