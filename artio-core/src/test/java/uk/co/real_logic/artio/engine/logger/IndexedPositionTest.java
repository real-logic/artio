/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.artio.FileSystemCorruptionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.engine.SectorFramer.SECTOR_SIZE;
import static uk.co.real_logic.artio.engine.logger.ErrorHandlerVerifier.verify;
import static uk.co.real_logic.artio.engine.logger.IndexedPositionReader.UNKNOWN_POSITION;

public class IndexedPositionTest
{
    private static final int SESSION_ID = 1;
    private static final int OTHER_SESSION_ID = 2;

    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private AtomicBuffer buffer = new UnsafeBuffer(new byte[2 * SECTOR_SIZE]);
    private IndexedPositionWriter writer = newWriter();
    private IndexedPositionReader reader = new IndexedPositionReader(buffer);

    @After
    public void noErrors()
    {
        verifyNoMoreInteractions(errorHandler);
    }

    @Test
    public void shouldReadWrittenPosition()
    {
        final int position = 10;

        indexed(position, SESSION_ID);

        hasPosition(position, SESSION_ID);
    }

    @Test
    public void shouldUpdateWrittenPosition()
    {
        int position = 10;

        indexed(position, SESSION_ID);

        position += 10;

        indexed(position, SESSION_ID);

        hasPosition(position, SESSION_ID);
    }

    @Test
    public void shouldUpdateWrittenPositionForGivenSession()
    {
        int position = 10;
        int otherPosition = 5;

        indexed(position, SESSION_ID);
        indexed(otherPosition, OTHER_SESSION_ID);

        hasPosition(position, SESSION_ID);
        hasPosition(otherPosition, OTHER_SESSION_ID);

        position += 20;
        otherPosition += 10;

        indexed(position, SESSION_ID);
        indexed(otherPosition, OTHER_SESSION_ID);

        hasPosition(position, SESSION_ID);
        hasPosition(otherPosition, OTHER_SESSION_ID);
    }

    @Test
    public void shouldNotReadMissingPosition()
    {
        hasPosition(UNKNOWN_POSITION, SESSION_ID);
    }

    @Test
    public void shouldValidateCorrectChecksums()
    {
        final int position = 10;

        indexed(position, SESSION_ID);

        writer.updateChecksums();

        newWriter();
        assertEquals(position, new IndexedPositionReader(buffer).indexedPosition(SESSION_ID));
    }

    @Test
    public void shouldDetectFileSystemCorruption()
    {
        final int position = 10;

        indexed(position, SESSION_ID);

        writer.updateChecksums();

        buffer.putBytes(5, new byte[300]);

        newWriter();

        verify(errorHandler, times(1), FileSystemCorruptionException.class);
    }

    @Test
    public void shouldNotReportFileSystemCorruptionWithNoWrittenRecords()
    {
        newWriter();

        noErrors();
    }

    private void indexed(final int position, final int sessionId)
    {
        writer.indexedUpTo(sessionId, position);
    }

    private void hasPosition(final long position, final int sessionId)
    {
        assertEquals(position, reader.indexedPosition(sessionId));
    }

    private IndexedPositionWriter newWriter()
    {
        return new IndexedPositionWriter(buffer, errorHandler, 0, "IndexedPosition");
    }
}