/*
 * Copyright 2015-2024 Real Logic Limited, Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_INDEX_CHECKSUM_ENABLED;
import static uk.co.real_logic.artio.engine.SectorFramer.SECTOR_SIZE;
import static uk.co.real_logic.artio.engine.logger.ErrorHandlerVerifier.verify;
import static uk.co.real_logic.artio.engine.logger.IndexedPositionReader.UNKNOWN_POSITION;

public class IndexedPositionTest
{
    private static final int SESSION_ID = 1;
    private static final int OTHER_SESSION_ID = 2;

    private static final int RECORDING_ID = 3;
    private static final int OTHER_RECORDING_ID = 4;

    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final AtomicBuffer buffer = new UnsafeBuffer(new byte[2 * SECTOR_SIZE]);
    private final IndexedPositionWriter writer = newWriter();
    private final IndexedPositionReader reader = new IndexedPositionReader(buffer);

    @After
    public void noErrors()
    {
        verifyNoMoreInteractions(errorHandler);
    }

    @Test
    public void shouldReadWrittenPosition()
    {
        final int position = 10;

        indexed(position, SESSION_ID, RECORDING_ID);

        hasPosition(position, RECORDING_ID);
    }

    @Test
    public void shouldUpdateWrittenPosition()
    {
        int position = 10;

        indexed(position, SESSION_ID, RECORDING_ID);

        position += 10;

        indexed(position, SESSION_ID, RECORDING_ID);

        hasPosition(position, RECORDING_ID);
    }

    @Test
    public void shouldUpdateWrittenPositionForGivenSession()
    {
        int position = 10;
        int otherPosition = 5;

        indexed(position, SESSION_ID, RECORDING_ID);
        indexed(otherPosition, OTHER_SESSION_ID, OTHER_RECORDING_ID);

        hasPosition(position, RECORDING_ID);
        hasPosition(otherPosition, OTHER_RECORDING_ID);

        queriesLastPosition(position, otherPosition);

        position += 20;
        otherPosition += 10;

        indexed(position, SESSION_ID, RECORDING_ID);
        indexed(otherPosition, OTHER_SESSION_ID, OTHER_RECORDING_ID);

        hasPosition(position, RECORDING_ID);
        hasPosition(otherPosition, OTHER_RECORDING_ID);

        queriesLastPosition(position, otherPosition);
    }

    private void queriesLastPosition(final int position, final int otherPosition)
    {
        final IndexedPositionConsumer consumer = mock(IndexedPositionConsumer.class);
        reader.readLastPosition(consumer);
        verify(consumer).accept(SESSION_ID, RECORDING_ID, position);
        verify(consumer).accept(OTHER_SESSION_ID, OTHER_RECORDING_ID, otherPosition);
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void shouldNotReadMissingPosition()
    {
        hasPosition(UNKNOWN_POSITION, RECORDING_ID);
    }

    @Test
    public void shouldValidateCorrectChecksums()
    {
        final int position = 10;

        indexed(position, SESSION_ID, RECORDING_ID);

        writer.updateChecksums();

        newWriter();
        assertEquals(position, new IndexedPositionReader(buffer).indexedPosition(RECORDING_ID));
    }

    @Test
    public void shouldDetectFileSystemCorruption()
    {
        final int position = 10;

        indexed(position, SESSION_ID, RECORDING_ID);

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

    private void indexed(final int position, final int sessionId, final int recordingId)
    {
        writer.indexedUpTo(sessionId, recordingId, position);
    }

    private void hasPosition(final long position, final int recordingId)
    {
        assertEquals(position, reader.indexedPosition(recordingId));
    }

    private IndexedPositionWriter newWriter()
    {
        return new IndexedPositionWriter(buffer, errorHandler, 0, "IndexedPosition",
            null, DEFAULT_INDEX_CHECKSUM_ENABLED);
    }
}
