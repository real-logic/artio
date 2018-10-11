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

import io.aeron.CommonContext;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;
import uk.co.real_logic.artio.messages.ManageSessionEncoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.GatewayProcess.ARCHIVE_REPLAY_STREAM;
import static uk.co.real_logic.artio.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.engine.EngineConfiguration.*;
import static uk.co.real_logic.artio.engine.logger.ReplayIndexDescriptor.*;
import static uk.co.real_logic.artio.engine.logger.Replayer.MOST_RECENT_MESSAGE;

// TODO: test case where we return less messages than expected.
@Ignore
public class ReplayIndexTest extends AbstractLogTest
{
    private static final String CHANNEL = CommonContext.IPC_CHANNEL;
    private static final long RECORDING_ID = 1L;

    private ByteBuffer indexBuffer = ByteBuffer.allocate(16 * 1024 + INITIAL_RECORD_OFFSET);
    private ExistingBufferFactory existingBufferFactory = mock(ExistingBufferFactory.class);
    private BufferFactory newBufferFactory = mock(BufferFactory.class);
    private ControlledFragmentHandler mockHandler = mock(ControlledFragmentHandler.class);
    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private ReplayIndex replayIndex;
    private AeronArchive mockArchive = mock(AeronArchive.class);
    private Subscription mockSubscription = mock(Subscription.class);
    private int totalMessages = (indexBuffer.capacity() - MessageHeaderEncoder.ENCODED_LENGTH) / RECORD_LENGTH;

    private UnsafeBuffer replayPositionBuffer = new UnsafeBuffer(new byte[REPLAY_POSITION_BUFFER_SIZE]);
    private IndexedPositionConsumer positionConsumer = mock(IndexedPositionConsumer.class);
    private IndexedPositionReader positionReader = new IndexedPositionReader(replayPositionBuffer);
    private ManageSessionEncoder logon = new ManageSessionEncoder();
    private RecordingIdLookup recordingIdLookup = mock(RecordingIdLookup.class);

    private void newReplayIndex()
    {
        replayIndex = new ReplayIndex(
            DEFAULT_LOG_FILE_DIR,
            STREAM_ID,
            DEFAULT_REPLAY_INDEX_FILE_SIZE,
            DEFAULT_LOGGER_CACHE_NUM_SETS,
            DEFAULT_LOGGER_CACHE_SET_SIZE,
            newBufferFactory,
            replayPositionBuffer,
            errorHandler,
            recordingIdLookup);
    }

    private ReplayQuery query;

    @Before
    public void setUp()
    {
        final File logFile = logFile(SESSION_ID);
        IoUtil.deleteIfExists(logFile);

        newReplayIndex();
        query = new ReplayQuery(
            DEFAULT_LOG_FILE_DIR,
            DEFAULT_LOGGER_CACHE_NUM_SETS,
            DEFAULT_LOGGER_CACHE_SET_SIZE,
            existingBufferFactory,
            OUTBOUND_LIBRARY_STREAM,
            new NoOpIdleStrategy(),
            mockArchive,
            CHANNEL,
            errorHandler);

        returnBuffer(indexBuffer, SESSION_ID);
        returnBuffer(ByteBuffer.allocate(16 * 1024), SESSION_ID_2);
        when(newBufferFactory.map(any(), anyInt())).thenReturn(indexBuffer);

        when(recordingIdLookup.getRecordingId(anyInt())).thenReturn(RECORDING_ID);
        when(mockArchive.replay(
            eq(RECORDING_ID),
            anyLong(),
            anyLong(),
            eq(CHANNEL),
            eq(ARCHIVE_REPLAY_STREAM))).thenReturn(mockSubscription);
        readsRequestedMessages();
    }

    private void readsRequestedMessages()
    {
        whenRead().then(inv -> inv.getArgument(1));
    }

    @After
    public void teardown()
    {
        replayIndex.close();
    }

    @Test
    public void shouldReturnLogEntriesMatchingQuery()
    {
        indexExampleMessage();

        final int msgCount = query();

        verifyMappedFile(SESSION_ID, 1);
        verifyMessagesRead(1);
        assertEquals(1, msgCount);
    }

    @Test
    public void shouldReadSecondRecord()
    {
        indexExampleMessage();

        final int endSequenceNumber = SEQUENCE_NUMBER + 1;
        indexExampleMessage(endSequenceNumber);

        final int msgCount = query(SEQUENCE_NUMBER, SEQUENCE_INDEX, endSequenceNumber, SEQUENCE_INDEX);

        assertEquals(2, msgCount);
        verifyMessagesRead(2);
    }

    @Test
    public void shouldReadRecordsFromBeforeARestart() throws IOException
    {
        indexExampleMessage();

        // Fake restarting the gateway
        final File logFile = logFile(SESSION_ID);
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

    @Test
    public void shouldReturnAllLogEntriesWhenMostResentMessageRequested()
    {
        indexExampleMessage();

        final int msgCount = query(SEQUENCE_NUMBER, SEQUENCE_INDEX, MOST_RECENT_MESSAGE, SEQUENCE_INDEX);

        verifyMappedFile(SESSION_ID, 1);
        verifyMessagesRead(1);
        assertEquals(1, msgCount);
    }

    @Test
    public void shouldReturnLogEntriesWhenMostResentMessageRequested()
    {
        indexExampleMessage();

        final int msgCount = query(SEQUENCE_NUMBER, SEQUENCE_INDEX, MOST_RECENT_MESSAGE, SEQUENCE_INDEX);

        verifyMappedFile(SESSION_ID, 1);
        verifyMessagesRead(1);
        assertEquals(1, msgCount);
    }

    @Test
    public void shouldNotReturnLogEntriesWithWrongSessionId()
    {
        indexExampleMessage();

        final int msgCount = query(SESSION_ID_2, SEQUENCE_NUMBER, SEQUENCE_INDEX, SEQUENCE_NUMBER, SEQUENCE_INDEX);

        assertEquals(0, msgCount);
        verifyMappedFile(SESSION_ID_2, 1);
        verifyNoMessageRead();
    }

    @Test
    public void shouldNotReturnLogEntriesWithOutOfRangeSequenceNumbers()
    {
        indexExampleMessage();

        final int msgCount = query(SESSION_ID, 1001, SEQUENCE_INDEX, 1002, SEQUENCE_INDEX);

        assertEquals(0, msgCount);
        verifyNoMessageRead();
    }

    @Test
    public void shouldQueryOverSequenceIndexBoundaries()
    {
        indexExampleMessage();

        readPositions(100L, 100L);

        final int nextSequenceIndex = SEQUENCE_INDEX + 1;
        final int endSequenceNumber = 1;

        bufferContainsExampleMessage(true, SESSION_ID, endSequenceNumber, nextSequenceIndex);
        indexRecord();

        final int msgCount = query(SEQUENCE_NUMBER, SEQUENCE_INDEX, endSequenceNumber, nextSequenceIndex);

        assertEquals(2, msgCount);
        verifyMessagesRead(2);
    }

    @Test
    public void shouldNotStopIndexingWhenBufferFull()
    {
        indexExampleMessage();

        final int beginSequenceNumber = 1;
        final int endSequenceNumber = 1_000;

        IntStream.rangeClosed(beginSequenceNumber, endSequenceNumber).forEach(this::indexExampleMessage);

        final int msgCount = query(beginSequenceNumber, SEQUENCE_INDEX, endSequenceNumber, SEQUENCE_INDEX);

        assertEquals(totalMessages, msgCount);
        verifyMessagesRead(totalMessages);
    }

    @Ignore // TODO: figure out how to do this test
    // another way now we read all the positions up front.
    @Test
    public void shouldCheckForWriterOverlap()
    {
        indexExampleMessage();

        whenRead().then((inv) ->
        {
            IntStream.range(SEQUENCE_NUMBER + 1, totalMessages + 4).forEach(this::indexExampleMessage);

            return 1;
        }).thenReturn(1);

        final int msgCount = query(SEQUENCE_NUMBER, SEQUENCE_INDEX, MOST_RECENT_MESSAGE, SEQUENCE_INDEX);

        assertEquals(totalMessages + 1, msgCount);
        verifyMessagesRead(totalMessages + 1);
    }

    @Test
    public void shouldUpdatePositionForIndexedRecord()
    {
        indexExampleMessage();

        positionReader.readLastPosition(positionConsumer);

        verify(positionConsumer, times(1)).accept(AERON_SESSION_ID, alignedEndPosition());
    }

    @Test
    public void shouldOnlyMapSessionFileOnce()
    {
        indexExampleMessage();

        indexRecord();

        verifyMappedFile(SESSION_ID);
    }

    @Test
    public void shouldRecordIndexesForMultipleSessions()
    {
        indexExampleMessage(SEQUENCE_NUMBER);

        bufferContainsExampleMessage(true, SESSION_ID_2, SEQUENCE_NUMBER, SEQUENCE_INDEX);
        indexRecord();

        verifyMappedFile(SESSION_ID);
        verifyMappedFile(SESSION_ID_2);
    }

    @Test
    public void shouldIgnoreOtherMessageTypes()
    {
        bufferContainsLogon();

        indexRecord();

        positionReader.readLastPosition(positionConsumer);

        verifyNoMoreInteractions(existingBufferFactory, positionConsumer);
    }

    private void bufferContainsLogon()
    {
        offset = START;

        logon
            .wrapAndApplyHeader(buffer, offset, header)
            .connection(CONNECTION_ID)
            .session(SESSION_ID);

        offset += header.encodedLength() + logon.encodedLength();
    }

    private void indexExampleMessage()
    {
        bufferContainsExampleMessage(true);
        indexRecord();
    }

    private void readPositions(final Long firstPosition, final Long... remainingPositions)
    {
        // TODO: whenRead().thenReturn(firstPosition, remainingPositions);
    }

    private OngoingStubbing<Integer> whenRead()
    {
        return when(mockSubscription.controlledPoll(any(ControlledFragmentHandler.class), anyInt()));
    }

    private void verifyNoMessageRead()
    {
        verifyNoMoreInteractions(mockSubscription);
    }

    private void verifyMessagesRead(final int number)
    {
        // TODO: get the number of messages right
        verify(mockSubscription, times(1))
            .controlledPoll(eq(mockHandler), eq(number));
    }

    private void returnBuffer(final ByteBuffer buffer, final long sessionId)
    {
        final File file = logFile(sessionId);
        when(existingBufferFactory.map(file)).thenReturn(buffer);
    }

    private void verifyMappedFile(final long sessionId, final int wantedNumberOfInvocations)
    {
        verify(existingBufferFactory, times(wantedNumberOfInvocations)).map(logFile(sessionId));
    }

    private void verifyMappedFile(final long sessionId)
    {
        verify(newBufferFactory).map(eq(logFile(sessionId)), anyInt());
    }

    private File logFile(final long sessionId)
    {
        return ReplayIndexDescriptor.logFile(DEFAULT_LOG_FILE_DIR, sessionId, STREAM_ID);
    }

    private void indexRecord()
    {
        replayIndex.indexRecord(buffer, START, fragmentLength(), STREAM_ID, AERON_SESSION_ID, alignedEndPosition());
    }

    private void indexExampleMessage(final int endSequenceNumber)
    {
        bufferContainsExampleMessage(true, SESSION_ID, endSequenceNumber, SEQUENCE_INDEX);
        indexRecord();
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
