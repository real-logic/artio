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

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.*;
import static uk.co.real_logic.fix_gateway.engine.logger.ReplayIndex.logFile;

public class ReplayQueryTest extends AbstractLogTest
{
    private ByteBuffer indexBuffer = ByteBuffer.allocate(16 * 1024);
    private ExistingBufferFactory mockBufferFactory = mock(ExistingBufferFactory.class);
    private LogHandler mockHandler = mock(LogHandler.class);
    private ArchiveReader mockReader = mock(ArchiveReader.class);
    private ReplayIndex replayIndex = new ReplayIndex(
        DEFAULT_LOG_FILE_DIR, DEFAULT_INDEX_FILE_SIZE, DEFAULT_LOGGER_CACHE_CAPACITY, (name, size) -> indexBuffer);

    private ReplayQuery query = new ReplayQuery(
        DEFAULT_LOG_FILE_DIR, DEFAULT_LOGGER_CACHE_CAPACITY, mockBufferFactory, mockReader);

    @Before
    public void setUp()
    {
        returnBuffer(indexBuffer, SESSION_ID);
        returnBuffer(ByteBuffer.allocate(16 * 1024), SESSION_ID_2);
        when(mockReader.read(anyInt(), anyLong(), any(LogHandler.class))).thenReturn(false);

        bufferContainsMessage(true);
        indexRecord();
    }

    @Test
    public void shouldReturnLogEntriesMatchingQuery()
    {
        final int msgCount = query.query(mockHandler, SESSION_ID, SEQUENCE_NUMBER, SEQUENCE_NUMBER);

        assertEquals(1, msgCount);
        verifyMappedFile(SESSION_ID, 1);
        verifyOneMessageRead();
    }

    @Test
    public void shouldNotReturnLogEntriesWithWrongSessionId()
    {
        final int msgCount = query.query(mockHandler, SESSION_ID_2, SEQUENCE_NUMBER, SEQUENCE_NUMBER);

        assertEquals(0, msgCount);
        verifyMappedFile(SESSION_ID_2, 1);
        verifyNoMessageRead();
    }

    @Test
    public void shouldNotReturnLogEntriesWithOutOfRangeSequenceNumbers()
    {
        final int msgCount = query.query(mockHandler, SESSION_ID, 1001, 1002);

        assertEquals(0, msgCount);
        verifyNoMessageRead();
    }

    @Test
    public void shouldStopWhenHandlerReturnsFalse()
    {
        indexSecondRecord();

        final int msgCount = query.query(mockHandler, SESSION_ID, SEQUENCE_NUMBER, SEQUENCE_NUMBER);

        assertEquals(1, msgCount);
        verifyOneMessageRead();
    }

    private void verifyNoMessageRead()
    {
        verifyNoMoreInteractions(mockReader);
    }

    private void verifyOneMessageRead()
    {
        verify(mockReader, times(1)).read(AERON_STREAM_ID, START, mockHandler);
    }

    private void returnBuffer(final ByteBuffer buffer, final long sessionId)
    {
        when(mockBufferFactory.map(logFile(DEFAULT_LOG_FILE_DIR, sessionId))).thenReturn(buffer);
    }

    private void verifyMappedFile(final long sessionId, final int wantedNumberOfInvocations)
    {
        verify(mockBufferFactory, times(wantedNumberOfInvocations)).map(logFile(DEFAULT_LOG_FILE_DIR, sessionId));
    }

    private void indexSecondRecord()
    {
        indexRecord();
    }

    private void indexRecord()
    {
        replayIndex.indexRecord(buffer, START, messageLength(), STREAM_ID, AERON_STREAM_ID);
    }
}
