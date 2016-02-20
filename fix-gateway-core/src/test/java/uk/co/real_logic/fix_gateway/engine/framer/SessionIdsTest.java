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
package uk.co.real_logic.fix_gateway.engine.framer;

import org.junit.Test;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.FileSystemCorruptionException;
import uk.co.real_logic.fix_gateway.session.SenderAndTargetSessionIdStrategy;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

public class SessionIdsTest
{
    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private AtomicBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(8 * 1024));
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

    @Test(expected = FileSystemCorruptionException.class)
    public void checksFileCorruption()
    {
        sessionIds.onLogon(bSession);
        sessionIds.onLogon(aSession);

        // corrupt buffer
        buffer.putBytes(8, new byte[1024]);

        new SessionIds(buffer, idStrategy, errorHandler);
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateSizeOfBuffer()
    {
        final AtomicBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(1024));
        new SessionIds(buffer, idStrategy, errorHandler);
    }

    // TODO: check wraps over buffers around

}
