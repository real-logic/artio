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
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.FileSystemCorruptionException;
import uk.co.real_logic.fix_gateway.engine.MappedFile;
import uk.co.real_logic.fix_gateway.session.SenderAndTargetSessionIdStrategy;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.co.real_logic.fix_gateway.engine.framer.SessionIds.LOWEST_VALID_SESSION_ID;

public class SessionIdsTest
{
    private static final int BUFFER_SIZE = 8 * 1024;

    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private AtomicBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(BUFFER_SIZE));
    private MappedFile mappedFile = mock(MappedFile.class);
    private SessionIdStrategy idStrategy = new SenderAndTargetSessionIdStrategy();
    private SessionIds sessionIds = newSessionIds(buffer);

    private Object aSession = idStrategy.onInitiatorLogon("a", null, null, "b");
    private Object bSession = idStrategy.onInitiatorLogon("b", null, null, "a");
    private Object cSession = idStrategy.onInitiatorLogon("c", null, null, "c");

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

        final SessionIds sessionIdsAfterRestart = newSessionIds(buffer);
        assertEquals(aId, sessionIdsAfterRestart.onLogon(aSession));
        assertEquals(bId, sessionIdsAfterRestart.onLogon(bSession));
    }

    @Test
    public void continuesIncrementingSessionIdsAfterRestart()
    {
        final long bId = sessionIds.onLogon(bSession);
        final long aId = sessionIds.onLogon(aSession);

        final SessionIds sessionIdsAfterRestart = newSessionIds(buffer);

        final long cId = sessionIdsAfterRestart.onLogon(cSession);
        assertValidSessionId(cId);
        assertNotEquals("C is a duplicate of A", aId, cId);
        assertNotEquals("C is a duplicate of B", bId, cId);
    }

    @Test(expected = FileSystemCorruptionException.class)
    public void checksFileCorruption()
    {
        sessionIds.onLogon(bSession);
        sessionIds.onLogon(aSession);

        // corrupt buffer
        buffer.putBytes(8, new byte[1024]);

        newSessionIds(buffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void validateSizeOfBuffer()
    {
        final AtomicBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(1024));
        newSessionIds(buffer);
    }

    @Test
    public void wrapsOverSectorBoundaries()
    {
        final int requiredNumberOfWritesToSpanSector = 300;

        Object compositeKey = null;
        long surrogateKey = 0;

        for (int i = 0; i < requiredNumberOfWritesToSpanSector; i++)
        {
            compositeKey = idStrategy.onInitiatorLogon("b" + i, null, null, "a" + i);
            surrogateKey = sessionIds.onLogon(compositeKey);
        }

        final SessionIds sessionIdsAfterRestart = newSessionIds(buffer);
        assertEquals(surrogateKey, sessionIdsAfterRestart.onLogon(compositeKey));
    }

    @Test
    public void resetsSessionIds()
    {
        final long aId = sessionIds.onLogon(aSession);
        sessionIds.onDisconnect(aId);

        sessionIds.reset(null);

        assertSessionIdsReset(aId, sessionIds);
    }

    @Test
    public void resetsSessionIdsFile()
    {
        final long aId = sessionIds.onLogon(aSession);
        sessionIds.onDisconnect(aId);

        sessionIds.reset(null);

        final SessionIds sessionIdsAfterRestart = newSessionIds(buffer);

        assertSessionIdsReset(aId, sessionIdsAfterRestart);
    }

    @Test
    public void copiesOldSessionIdFile() throws IOException
    {
        final File backupLocation = File.createTempFile("sessionIds", "tmp");
        try
        {
            final long aId = sessionIds.onLogon(aSession);
            sessionIds.onDisconnect(aId);

            final byte[] oldData = new byte[BUFFER_SIZE];
            buffer.getBytes(0, oldData);

            sessionIds.reset(backupLocation);

            verify(mappedFile).transferTo(backupLocation);
        }
        finally
        {
            IoUtil.deleteIfExists(backupLocation);
        }
    }

    private void assertSessionIdsReset(final long aId, final SessionIds sessionIds)
    {
        final long bId = sessionIds.onLogon(bSession);
        final long newAId = sessionIds.onLogon(aSession);
        assertValidSessionId(bId);
        assertValidSessionId(newAId);
        assertEquals("Session Ids haven't been reset", aId, bId);
        assertNotEquals("Session Ids haven't been reset", aId, newAId);
    }

    /*final AtomicBuffer backupBuffer = new UnsafeBuffer(ByteBuffer.allocate(BUFFER_SIZE));
    final File backupLocation = new File();*/

    private void assertValidSessionId(final long cId)
    {
        assertThat(cId, greaterThanOrEqualTo(LOWEST_VALID_SESSION_ID));
    }

    private SessionIds newSessionIds(final AtomicBuffer buffer)
    {
        when(mappedFile.buffer()).thenReturn(buffer);
        return new SessionIds(mappedFile, idStrategy, errorHandler);
    }

}
