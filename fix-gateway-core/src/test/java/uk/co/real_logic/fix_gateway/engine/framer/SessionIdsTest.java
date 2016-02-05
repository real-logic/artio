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
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.session.SenderAndTargetSessionIdStrategy;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SessionIdsTest
{
    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private AtomicBuffer buffer = new UnsafeBuffer(new byte[1024]);
    private SessionIdStrategy idStrategy = new SenderAndTargetSessionIdStrategy();
    private SessionIds sessionIds = new SessionIds(buffer, idStrategy, errorHandler);

    private Object aSession = idStrategy.onInitiatorLogon("a", null, null, "b");
    private Object bSession = idStrategy.onInitiatorLogon("b", null, null, "a");

    @Test
    public void sessionIdsAreUnique()
    {
        assertNotEquals(sessionIds.onLogon(aSession), sessionIds.onLogon(bSession));
    }

    @Test
    public void findsDuplicateSessions()
    {
        sessionIds.onLogon(aSession);

        assertEquals(SessionIds.DUPLICATE_SESSION, sessionIds.onLogon(aSession));
    }

    @Test
    public void handsOutSameSessionIdAfterDisconnect()
    {
        final long sessionId = sessionIds.onLogon(aSession);
        sessionIds.onDisconnect(sessionId);

        assertEquals(sessionId, sessionIds.onLogon(aSession));
    }

    @Test
    public void persistsSessionIdsOverARestart()
    {
        final long bId = sessionIds.onLogon(bSession);
        final long aId = sessionIds.onLogon(aSession);

        final SessionIds sessionIdsAfterRestart = new SessionIds(buffer, idStrategy, errorHandler);
        assertEquals(aId, sessionIdsAfterRestart.onLogon(aSession));
        assertEquals(bId, sessionIdsAfterRestart.onLogon(bSession));
    }

    @Test
    public void logsErrorWhenBufferInsufficientlySized()
    {
        final AtomicBuffer buffer = new UnsafeBuffer(new byte[20]);
        final SessionIds sessionIds = new SessionIds(buffer, idStrategy, errorHandler);
        sessionIds.onLogon(bSession);

        verify(errorHandler, times(1)).onError(any());
    }

}
