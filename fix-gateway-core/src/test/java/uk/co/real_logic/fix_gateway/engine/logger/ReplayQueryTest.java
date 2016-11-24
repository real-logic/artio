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

import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.*;
import static uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader.UNKNOWN_SESSION;
import static uk.co.real_logic.fix_gateway.engine.logger.ReplayIndex.logFile;
import static uk.co.real_logic.fix_gateway.engine.logger.Replayer.MOST_RECENT_MESSAGE;

public class ReplayQueryTest extends AbstractLogTest
{
    private ByteBuffer indexBuffer = ByteBuffer.allocate(16 * 1024);
    private ExistingBufferFactory mockBufferFactory = mock(ExistingBufferFactory.class);
    private ControlledFragmentHandler mockHandler = mock(ControlledFragmentHandler.class);
    private ArchiveReader mockReader = mock(ArchiveReader.class);
    private ArchiveReader.SessionReader mockSessionReader = mock(ArchiveReader.SessionReader.class);
    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private ReplayIndex replayIndex;

    private void newReplayIndex()
    {
        replayIndex = new ReplayIndex(
            DEFAULT_LOG_FILE_DIR,
            STREAM_ID,
            DEFAULT_INDEX_FILE_SIZE,
            DEFAULT_LOGGER_CACHE_NUM_SETS,
            DEFAULT_LOGGER_CACHE_SET_SIZE,
            (name, size) -> indexBuffer,
            buffer,
            errorHandler);
    }

    private ReplayQuery query;

    @Before
    public void setUp()
    {
        newReplayIndex();
        query = new ReplayQuery(
            DEFAULT_LOG_FILE_DIR,
            DEFAULT_LOGGER_CACHE_NUM_SETS,
            DEFAULT_LOGGER_CACHE_SET_SIZE,
            mockBufferFactory,
            mockReader,
            OUTBOUND_LIBRARY_STREAM);

        returnBuffer(indexBuffer, SESSION_ID);
        returnBuffer(ByteBuffer.allocate(16 * 1024), SESSION_ID_2);

        when(mockReader.session(anyInt())).thenReturn(mockSessionReader);
        when(mockSessionReader.read(anyLong(), any(ControlledFragmentHandler.class)))
            .thenReturn(100L, (long) UNKNOWN_SESSION);

        bufferContainsMessage(true);
        indexRecord();
    }

    @Test
    public void shouldReturnLogEntriesMatchingQuery()
    {
        final int msgCount = query.query(mockHandler, SESSION_ID, SEQUENCE_NUMBER, SEQUENCE_NUMBER);

        verifyMappedFile(SESSION_ID, 1);
        verifyMessagesRead(1);
        assertEquals(1, msgCount);
    }

    @Test
    public void shouldReadRecordsFromBeforeARestart() throws IOException
    {
        // Fake restarting the gateway
        final File logFile = logFile(DEFAULT_LOG_FILE_DIR, SESSION_ID, STREAM_ID);
        IoUtil.ensureDirectoryExists(new File(DEFAULT_LOG_FILE_DIR), DEFAULT_LOG_FILE_DIR);
        logFile.createNewFile();
        try
        {
            newReplayIndex();

            bufferContainsMessage(false, SESSION_ID, SEQUENCE_NUMBER + 1);
            indexSecondRecord();

            final int msgCount = query.query(mockHandler, SESSION_ID, SEQUENCE_NUMBER, SEQUENCE_NUMBER);

            verifyMappedFile(SESSION_ID, 1);
            verifyMessagesRead(1);
            assertEquals(1, msgCount);
        }
        finally
        {
            IoUtil.delete(new File(DEFAULT_LOG_FILE_DIR), false);
        }

    }

    @Test
    public void shouldReturnAllLogEntriesWhenMostResentMessageRequested()
    {
        final int msgCount = query.query(mockHandler, SESSION_ID, SEQUENCE_NUMBER, MOST_RECENT_MESSAGE);

        verifyMappedFile(SESSION_ID, 1);
        verifyMessagesRead(1);
        assertEquals(1, msgCount);
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
        verifyMessagesRead(2);
    }

    private void verifyNoMessageRead()
    {
        verifyNoMoreInteractions(mockSessionReader);
    }

    private void verifyMessagesRead(final int number)
    {
        verify(mockSessionReader, times(number)).read(START, mockHandler);
    }

    private void returnBuffer(final ByteBuffer buffer, final long sessionId)
    {
        when(mockBufferFactory.map(logFile(DEFAULT_LOG_FILE_DIR, sessionId, STREAM_ID))).thenReturn(buffer);
    }

    private void verifyMappedFile(final long sessionId, final int wantedNumberOfInvocations)
    {
        verify(mockBufferFactory, times(wantedNumberOfInvocations))
            .map(logFile(DEFAULT_LOG_FILE_DIR, sessionId, STREAM_ID));
    }

    private void indexSecondRecord()
    {
        indexRecord();
    }

    private void indexRecord()
    {
        replayIndex.indexRecord(buffer, START, fragmentLength(), STREAM_ID, AERON_SESSION_ID, alignedEndPosition());
    }
}
