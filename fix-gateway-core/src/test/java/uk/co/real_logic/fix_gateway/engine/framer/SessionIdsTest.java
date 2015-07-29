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
package uk.co.real_logic.fix_gateway.engine.framer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SessionIdsTest
{
    private SessionIds sessionIds = new SessionIds();

    @Test
    public void sessionIdsAreUnique()
    {
        assertNotEquals(sessionIds.onLogon("a"), sessionIds.onLogon("b"));
    }

    @Test
    public void findsDuplicateSessions()
    {
        sessionIds.onLogon("a");

        assertEquals(SessionIds.DUPLICATE_SESSION, sessionIds.onLogon("a"));
    }

    @Test
    public void handsOutSameSessionIdAfterDisconnect()
    {
        final long sessionId = sessionIds.onLogon("a");
        sessionIds.onDisconnect(sessionId);

        assertEquals(sessionId, sessionIds.onLogon("a"));
    }

}
