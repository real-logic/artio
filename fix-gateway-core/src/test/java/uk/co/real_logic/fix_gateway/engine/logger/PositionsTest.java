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
package uk.co.real_logic.fix_gateway.engine.logger;

import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.engine.logger.PositionsReader.UNKNOWN_POSITION;

public class PositionsTest
{
    private static final int AERON_SESSION_ID = 1;

    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private AtomicBuffer buffer = new UnsafeBuffer(new byte[1024]);
    private PositionsWriter writer = new PositionsWriter(buffer, errorHandler);
    private PositionsReader reader = new PositionsReader(buffer);

    @Test
    public void shouldReadWrittenPosition()
    {
        final int position = 10;

        indexed(position);

        hasPosition(position);
    }

    @Test
    public void shouldUpdateWrittenPosition()
    {
        int position = 10;

        indexed(position);

        position += 10;

        indexed(position);

        hasPosition(position);
    }

    @Test
    public void shouldNotReadMissingPosition()
    {
        assertEquals(UNKNOWN_POSITION, reader.indexedPosition(AERON_SESSION_ID));
    }

    private void indexed(final int position)
    {
        writer.indexedUpTo(AERON_SESSION_ID, position);
    }

    private void hasPosition(final int position)
    {
        assertEquals(position, reader.indexedPosition(AERON_SESSION_ID));
    }

    @After
    public void noErrors()
    {
        verify(errorHandler, never()).onError(any());
    }
}
