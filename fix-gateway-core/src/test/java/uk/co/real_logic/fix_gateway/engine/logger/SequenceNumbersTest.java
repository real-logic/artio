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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.fix_gateway.engine.logger.SequenceNumbers.NONE;

/**
 * .
 */
public class SequenceNumbersTest extends AbstractLogTest
{

    private AtomicBuffer tableBuffer = new UnsafeBuffer(new byte[16 * 1024]);
    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private SequenceNumbers writer = SequenceNumbers.forWriting(tableBuffer, errorHandler);
    private SequenceNumbers reader = SequenceNumbers.forReading(tableBuffer, errorHandler);

    @Test
    public void shouldNotInitiallyKnowASequenceNumber()
    {
        assertLastKnownSequenceNumberIs(NONE, SESSION_ID);
    }

    @Test
    public void shouldStashNewSequenceNumber()
    {
        bufferContainsMessage(true);

        indexRecord();

        assertLastKnownSequenceNumberIs(SEQUENCE_NUMBER, SESSION_ID);
    }

    @Test
    public void shouldStashSequenceNumbersAgainstASessionId()
    {
        bufferContainsMessage(true);

        indexRecord();

        assertLastKnownSequenceNumberIs(NONE, SESSION_ID_2);
    }

    @Test
    public void shouldUpdateSequenceNumber()
    {
        final int updatedSequenceNumber = 8;

        bufferContainsMessage(true);

        indexRecord();

        bufferContainsMessage(true, SESSION_ID, updatedSequenceNumber);

        indexRecord();

        assertLastKnownSequenceNumberIs(updatedSequenceNumber, SESSION_ID);
    }

    @After
    public void verifyNoErrors()
    {
        verify(errorHandler, never()).onError(any());
    }

    private void indexRecord()
    {
        writer.indexRecord(buffer, START, messageLength(), STREAM_ID, AERON_STREAM_ID);
    }

    private void assertLastKnownSequenceNumberIs(final int expectedSequenceNumber, final long sessionId)
    {
        final int number = reader.lastKnownSequenceNumber(sessionId);
        assertEquals(expectedSequenceNumber, number);
    }

}
