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

import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.FileSystemCorruptionException;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.engine.MappedFile;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.engine.framer.SessionIds.LOWEST_VALID_SESSION_ID;

public class SessionIdsTest
{
    private static final int BUFFER_SIZE = 8 * 1024;
    private static final int SEQUENCE_INDEX = 1;

    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private AtomicBuffer buffer = new UnsafeBuffer(ByteBuffer.allocate(BUFFER_SIZE));
    private MappedFile mappedFile = mock(MappedFile.class);
    private SessionIdStrategy idStrategy = SessionIdStrategy.senderAndTarget();
    private SessionIds sessionIds = newSessionIds(buffer);

    private CompositeKey aSession = idStrategy.onLogon("a", null, null, "b");
    private CompositeKey bSession = idStrategy.onLogon("b", null, null, "a");
    private CompositeKey cSession = idStrategy.onLogon("c", null, null, "c");

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
        final SessionContext sessionContext = sessionIds.onLogon(aSession);
        sessionIds.onDisconnect(sessionContext.sessionId());

        assertEquals(sessionContext, sessionIds.onLogon(aSession));
    }

    @Test
    public void persistsSessionIdsOverARestart()
    {
        final SessionContext bContext = sessionIds.onLogon(bSession);
        final SessionContext aContext = sessionIds.onLogon(aSession);

        final SessionIds sessionIdsAfterRestart = newSessionIds(buffer);
        assertEquals(aContext, sessionIdsAfterRestart.onLogon(aSession));
        assertEquals(bContext, sessionIdsAfterRestart.onLogon(bSession));
    }

    @Test
    public void continuesIncrementingSessionIdsAfterRestart()
    {
        final SessionContext bContext = sessionIds.onLogon(bSession);
        final SessionContext aContext = sessionIds.onLogon(aSession);

        final SessionIds sessionIdsAfterRestart = newSessionIds(buffer);

        final SessionContext cContext = sessionIdsAfterRestart.onLogon(cSession);
        assertValidSessionId(cContext.sessionId());
        assertNotEquals("C is a duplicate of A", aContext, cContext);
        assertNotEquals("C is a duplicate of B", bContext, cContext);
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

        CompositeKey compositeKey = null;
        SessionContext surrogateKey = null;

        for (int i = 0; i < requiredNumberOfWritesToSpanSector; i++)
        {
            compositeKey = idStrategy.onLogon("b" + i, null, null, "a" + i);
            surrogateKey = sessionIds.onLogon(compositeKey);
        }

        final SessionIds sessionIdsAfterRestart = newSessionIds(buffer);
        assertEquals(surrogateKey, sessionIdsAfterRestart.onLogon(compositeKey));
    }

    @Test
    public void resetsSessionIds()
    {
        final SessionContext aContext = sessionIds.onLogon(aSession);
        sessionIds.onDisconnect(aContext.sessionId());

        sessionIds.reset(null);

        assertSessionIdsReset(aContext, sessionIds);

        verifyNoBackUp();
    }

    @Test
    public void resetsSessionIdsFile()
    {
        final SessionContext aContext = sessionIds.onLogon(aSession);
        sessionIds.onDisconnect(aContext.sessionId());

        sessionIds.reset(null);

        final SessionIds sessionIdsAfterRestart = newSessionIds(buffer);

        assertSessionIdsReset(aContext, sessionIdsAfterRestart);

        verifyNoBackUp();
    }

    @Test
    public void copiesOldSessionIdFile() throws IOException
    {
        final File backupLocation = File.createTempFile("sessionIds", "tmp");
        try
        {
            final SessionContext aContext = sessionIds.onLogon(aSession);
            sessionIds.onDisconnect(aContext.sessionId());

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

    @Test
    public void handsOutSameSessionIdAfterTakingOverAsLeader()
    {
        final long sessionId = 123;
        final HeaderDecoder header = mock(HeaderDecoder.class);
        when(header.senderCompIDAsString()).thenReturn(aSession.senderCompId());
        when(header.targetCompIDAsString()).thenReturn(aSession.targetCompId());

        sessionIds.onSentFollowerLogon(header, sessionId, SEQUENCE_INDEX);

        assertEquals(sessionId, sessionIds.onLogon(aSession).sessionId());
    }

    private void verifyNoBackUp()
    {
        verify(mappedFile, never()).transferTo(any());
    }

    private void assertSessionIdsReset(final SessionContext aContext, final SessionIds sessionIds)
    {
        final SessionContext bContext = sessionIds.onLogon(bSession);
        final SessionContext newAContext = sessionIds.onLogon(aSession);
        assertValidSessionId(bContext.sessionId());
        assertValidSessionId(newAContext.sessionId());
        assertEquals("Session Ids haven't been reset", aContext, bContext);
        assertNotEquals("Session Ids haven't been reset", aContext, newAContext);
    }

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
