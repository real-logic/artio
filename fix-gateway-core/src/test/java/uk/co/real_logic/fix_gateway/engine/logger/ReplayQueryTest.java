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
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.storage.messages.ReplayIndexRecordEncoder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.*;
import static uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader.UNKNOWN_SESSION;
import static uk.co.real_logic.fix_gateway.engine.logger.ReplayIndexDescriptor.INITIAL_RECORD_OFFSET;
import static uk.co.real_logic.fix_gateway.engine.logger.Replayer.MOST_RECENT_MESSAGE;

public class ReplayQueryTest extends AbstractLogTest
{
    private ByteBuffer indexBuffer = ByteBuffer.allocate(16 * 1024 + INITIAL_RECORD_OFFSET);
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
            DEFAULT_REPLAY_INDEX_FILE_SIZE,
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
        final File logFile = logFile();
        IoUtil.deleteIfExists(logFile);

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
        whenRead().thenReturn(100L);

        bufferContainsExampleMessage(true);
        indexRecord();
    }

    private void readPositions(final Long firstPosition, final Long... remainingPositions)
    {
        whenRead().thenReturn(firstPosition, remainingPositions);
    }

    private OngoingStubbing<Long> whenRead()
    {
        return when(mockSessionReader.read(anyLong(), any(ControlledFragmentHandler.class)));
    }

    @Test
    public void shouldReturnLogEntriesMatchingQuery()
    {
        final int msgCount = query();

        verifyMappedFile(SESSION_ID, 1);
        verifyMessagesRead(1);
        assertEquals(1, msgCount);
    }

    // TODO: make capacity a power of two

    @Test
    public void shouldReadSecondRecord()
    {
        final int endSequenceNumber = SEQUENCE_NUMBER + 1;
        bufferContainsExampleMessage(
            true, SESSION_ID, endSequenceNumber, SEQUENCE_INDEX);
        indexRecord();

        final int msgCount = query(SEQUENCE_NUMBER, SEQUENCE_INDEX, endSequenceNumber, SEQUENCE_INDEX);

        assertEquals(2, msgCount);
        verifyMessagesRead(2);
    }

    @Test
    public void shouldReadRecordsFromBeforeARestart() throws IOException
    {
        // Fake restarting the gateway
        final File logFile = logFile();
        IoUtil.ensureDirectoryExists(new File(DEFAULT_LOG_FILE_DIR), DEFAULT_LOG_FILE_DIR);
        logFile.createNewFile();
        try
        {
            newReplayIndex();

            bufferContainsExampleMessage(false, SESSION_ID, SEQUENCE_NUMBER + 1, SEQUENCE_INDEX);
            indexRecord();

            final int msgCount = query();

            verifyMappedFile(SESSION_ID, 1);
            verifyMessagesRead(1);
            assertEquals(1, msgCount);
        }
        finally
        {
            IoUtil.delete(new File(DEFAULT_LOG_FILE_DIR), false);
        }
    }

    private File logFile()
    {
        return ReplayIndexDescriptor.logFile(DEFAULT_LOG_FILE_DIR, SESSION_ID, STREAM_ID);
    }

    @Test
    public void shouldReturnAllLogEntriesWhenMostResentMessageRequested()
    {
        final int msgCount = query(SEQUENCE_NUMBER, SEQUENCE_INDEX, MOST_RECENT_MESSAGE, SEQUENCE_INDEX);

        verifyMappedFile(SESSION_ID, 1);
        verifyMessagesRead(1);
        assertEquals(1, msgCount);
    }

    @Test
    public void shouldReturnLogEntriesWhenMostResentMessageRequested()
    {
        final int msgCount = query(SEQUENCE_NUMBER, SEQUENCE_INDEX, MOST_RECENT_MESSAGE, SEQUENCE_INDEX);

        verifyMappedFile(SESSION_ID, 1);
        verifyMessagesRead(1);
        assertEquals(1, msgCount);
    }

    @Test
    public void shouldNotReturnLogEntriesWithWrongSessionId()
    {
        final int msgCount = query(SESSION_ID_2, SEQUENCE_NUMBER, SEQUENCE_INDEX, SEQUENCE_NUMBER, SEQUENCE_INDEX);

        assertEquals(0, msgCount);
        verifyMappedFile(SESSION_ID_2, 1);
        verifyNoMessageRead();
    }

    @Test
    public void shouldNotReturnLogEntriesWithOutOfRangeSequenceNumbers()
    {
        final int msgCount = query(SESSION_ID, 1001, SEQUENCE_INDEX, 1002, SEQUENCE_INDEX);

        assertEquals(0, msgCount);
        verifyNoMessageRead();
    }

    @Test
    public void shouldStopWhenSessionReaderReturnsLowPosition()
    {
        readPositions(100L, (long)UNKNOWN_SESSION);
        indexRecord();

        final int msgCount = query();

        assertEquals(1, msgCount);
        verifyMessagesRead(2);
    }

    @Test
    public void shouldQueryOverSequenceIndexBoundaries()
    {
        readPositions(100L, 100L);

        final int nextSequenceIndex = SEQUENCE_INDEX + 1;
        final int endSequenceNumber = 1;

        bufferContainsExampleMessage(true, SESSION_ID, endSequenceNumber, nextSequenceIndex);
        indexRecord();

        final int msgCount = query(SEQUENCE_NUMBER, SEQUENCE_INDEX, endSequenceNumber, nextSequenceIndex);

        assertEquals(2, msgCount);
        verifyMessagesRead(2);
    }

    @Ignore
    @Test
    public void shouldNotStopIndexingWhenBufferFull()
    {
        whenRead().thenReturn(100L);

        final int beginSequenceNumber = 1;
        final int endSequenceNumber = 2_000;
        final int totalMessages =
            (indexBuffer.capacity() - MessageHeaderEncoder.ENCODED_LENGTH) / ReplayIndexRecordEncoder.BLOCK_LENGTH;

        for (int sequenceNumber = beginSequenceNumber; sequenceNumber <= endSequenceNumber; sequenceNumber++)
        {
            bufferContainsExampleMessage(false, SESSION_ID, sequenceNumber, SEQUENCE_INDEX);
            indexRecord();
        }

        final int msgCount = query(beginSequenceNumber, SEQUENCE_INDEX, endSequenceNumber, SEQUENCE_INDEX);

        assertEquals(totalMessages, msgCount);
        verifyMessagesRead(totalMessages);
    }

    // TODO: fix potential record corruption
    // TODO: out of bounds query failing + counter returning
    // TODO: reloading from a full buffer and continuing

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
        final File file = ReplayIndexDescriptor.logFile(DEFAULT_LOG_FILE_DIR, sessionId, STREAM_ID);
        when(mockBufferFactory.map(file)).thenReturn(buffer);
    }

    private void verifyMappedFile(final long sessionId, final int wantedNumberOfInvocations)
    {
        verify(mockBufferFactory, times(wantedNumberOfInvocations))
            .map(ReplayIndexDescriptor.logFile(DEFAULT_LOG_FILE_DIR, sessionId, STREAM_ID));
    }

    private void indexRecord()
    {
        replayIndex.indexRecord(buffer, START, fragmentLength(), STREAM_ID, AERON_SESSION_ID, alignedEndPosition());
    }

    private int query()
    {
        return query(SEQUENCE_NUMBER, SEQUENCE_INDEX, SEQUENCE_NUMBER, SEQUENCE_INDEX);
    }

    private int query(
        final int beginSequenceNumber,
        final int beginSequenceIndex,
        final int endSequenceNumber,
        final int endSequenceIndex)
    {
        return query(SESSION_ID, beginSequenceNumber, beginSequenceIndex, endSequenceNumber, endSequenceIndex);
    }

    private int query(
        final long sessionId,
        final int beginSequenceNumber,
        final int beginSequenceIndex,
        final int endSequenceNumber,
        final int endSequenceIndex)
    {
        return query.query(
            mockHandler, sessionId, beginSequenceNumber, beginSequenceIndex, endSequenceNumber, endSequenceIndex);
    }
}
