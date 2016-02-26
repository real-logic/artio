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
package uk.co.real_logic.fix_gateway.engine.logger;

import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.engine.MappedFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class SequenceNumberIndexTest extends AbstractLogTest
{
    private static final int BUFFER_SIZE = 16 * 1024;

    private AtomicBuffer inMemoryBuffer = newBuffer();

    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private SequenceNumberIndexWriter writer = newWriter(inMemoryBuffer);

    private SequenceNumberIndexReader reader = new SequenceNumberIndexReader(inMemoryBuffer);

    @Test
    public void shouldNotInitiallyKnowASequenceNumber()
    {
        assertLastKnownSequenceNumberIs(SequenceNumberIndexReader.UNKNOWN_SESSION, SESSION_ID);
    }

    @Test
    public void shouldStashNewSequenceNumber()
    {
        bufferContainsMessage(true);

        indexRecord(START);

        assertLastKnownSequenceNumberIs(SEQUENCE_NUMBER, SESSION_ID);
    }

    @Test
    public void shouldStashSequenceNumbersAgainstASessionId()
    {
        bufferContainsMessage(true);

        indexRecord(START);

        assertLastKnownSequenceNumberIs(SequenceNumberIndexReader.UNKNOWN_SESSION, SESSION_ID_2);
    }

    @Test
    public void shouldUpdateSequenceNumber()
    {
        final int updatedSequenceNumber = 8;

        bufferContainsMessage(true);

        indexRecord(START);

        bufferContainsMessage(true, SESSION_ID, updatedSequenceNumber);

        indexRecord(START + fragmentLength());

        assertLastKnownSequenceNumberIs(updatedSequenceNumber, SESSION_ID);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldValidateBufferItReadsFrom()
    {
        final AtomicBuffer tableBuffer = newBuffer();

        new SequenceNumberIndexReader(tableBuffer);
    }

    @Test
    public void shouldSaveIndexUponClose()
    {
        bufferContainsMessage(true);

        indexRecord(START);

        writer.close();

        final AtomicBuffer inMemoryBuffer = newBuffer();
        newWriter(inMemoryBuffer);
        final SequenceNumberIndexReader newReader = new SequenceNumberIndexReader(inMemoryBuffer);
        assertLastKnownSequenceNumberIs(SEQUENCE_NUMBER, SESSION_ID, newReader);
    }

    @Test
    public void shouldSaveIndexUponRotate()
    {
        final int requiredMessagesToRoll = 3;
        for (int i = 0; i <= requiredMessagesToRoll; i++)
        {
            bufferContainsMessage(true, SESSION_ID, SEQUENCE_NUMBER + i);
            indexRecord(START + (i * fragmentLength()));
        }

        final AtomicBuffer inMemoryBuffer = newBuffer();
        newWriter(inMemoryBuffer);
        final SequenceNumberIndexReader newReader = new SequenceNumberIndexReader(inMemoryBuffer);
        assertLastKnownSequenceNumberIs(SEQUENCE_NUMBER + requiredMessagesToRoll, SESSION_ID, newReader);
    }

    @After
    public void verifyNoErrors()
    {
        writer.close();
        assertTrue(writer.clear());
        verify(errorHandler, never()).onError(any());
    }

    private SequenceNumberIndexWriter newWriter(final AtomicBuffer inMemoryBuffer)
    {
        final MappedFile indexFile = MappedFile.map(IoUtil.tmpDirName() + "/SequenceNumberIndex", BUFFER_SIZE);
        return new SequenceNumberIndexWriter(inMemoryBuffer, indexFile, errorHandler);
    }

    private UnsafeBuffer newBuffer()
    {
        return new UnsafeBuffer(new byte[BUFFER_SIZE]);
    }

    private void indexRecord(final int position)
    {
        writer.indexRecord(buffer, START, fragmentLength(), STREAM_ID, AERON_SESSION_ID, position);
    }

    private void assertLastKnownSequenceNumberIs(final int expectedSequenceNumber, final long sessionId)
    {
        assertLastKnownSequenceNumberIs(expectedSequenceNumber, sessionId, reader);
    }

    private void assertLastKnownSequenceNumberIs(
        final long expectedSequenceNumber,
        final long sessionId,
        final SequenceNumberIndexReader reader)
    {
        final int number = reader.lastKnownSequenceNumber(sessionId);
        assertEquals(expectedSequenceNumber, number);
    }

}
