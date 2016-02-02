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
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndex.STREAM_POSITION_BEHIND;
import static uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndex.UNKNOWN_SESSION;

public class SequenceNumberIndexTest extends AbstractLogTest
{

    private AtomicBuffer tableBuffer = new UnsafeBuffer(new byte[16 * 1024]);
    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private SequenceNumberIndex writer = SequenceNumberIndex.forWriting(tableBuffer, errorHandler);
    private SequenceNumberIndex reader = SequenceNumberIndex.forReading(tableBuffer, errorHandler);

    @Test
    public void shouldNotInitiallyKnowASequenceNumber()
    {
        assertLastKnownSequenceNumberIs(UNKNOWN_SESSION, SESSION_ID);
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

        assertLastKnownSequenceNumberIs(UNKNOWN_SESSION, SESSION_ID_2);
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

    @Test
    public void shouldWaitForSequenceNumberToBeIndexed()
    {
        final int updatedSequenceNumber = 8;
        final int requiredStreamPosition = 150;

        bufferContainsMessage(true);

        indexRecord(START);

        assertLastKnownSequenceNumberIs(STREAM_POSITION_BEHIND, SESSION_ID, requiredStreamPosition);

        bufferContainsMessage(true, SESSION_ID, updatedSequenceNumber);

        indexRecord(START + fragmentLength());

        assertLastKnownSequenceNumberIs(updatedSequenceNumber, SESSION_ID, requiredStreamPosition);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldValidateBufferItReadsFrom()
    {
        final AtomicBuffer tableBuffer = new UnsafeBuffer(new byte[16 * 1024]);

        SequenceNumberIndex.forReading(tableBuffer, errorHandler);
    }

    @After
    public void verifyNoErrors()
    {
        verify(errorHandler, never()).onError(any());
    }

    private void indexRecord(final int position)
    {
        writer.indexRecord(buffer, START, fragmentLength(), STREAM_ID, AERON_STREAM_ID, position);
    }

    private void assertLastKnownSequenceNumberIs(
        final int expectedSequenceNumber, final long sessionId)
    {
        assertLastKnownSequenceNumberIs(expectedSequenceNumber, sessionId, 0);
    }

    private void assertLastKnownSequenceNumberIs(
        final int expectedSequenceNumber,
        final long sessionId,
        final int requiredStreamPosition)
    {
        final int number = reader.lastKnownSequenceNumber(sessionId, requiredStreamPosition);
        assertEquals(expectedSequenceNumber, number);
    }

}
