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

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.artio.TestFixtures;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.messages.ManageSessionEncoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.TestFixtures.cleanupMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.largeTestReqId;
import static uk.co.real_logic.artio.engine.EngineConfiguration.*;
import static uk.co.real_logic.artio.engine.logger.ReplayIndexDescriptor.*;
import static uk.co.real_logic.artio.engine.logger.Replayer.MOST_RECENT_MESSAGE;

public class ReplayIndexTest extends AbstractLogTest
{
    private static final String CHANNEL = CommonContext.IPC_CHANNEL;

    private ByteBuffer indexBuffer = ByteBuffer.allocate(16 * 1024 + INITIAL_RECORD_OFFSET);
    private ExistingBufferFactory existingBufferFactory = mock(ExistingBufferFactory.class);
    private BufferFactory newBufferFactory = mock(BufferFactory.class);
    private ReplayIndex replayIndex;
    private int totalMessages = (indexBuffer.capacity() - MessageHeaderEncoder.ENCODED_LENGTH) / RECORD_LENGTH;

    private UnsafeBuffer replayPositionBuffer = new UnsafeBuffer(new byte[REPLAY_POSITION_BUFFER_SIZE]);
    private IndexedPositionConsumer positionConsumer = mock(IndexedPositionConsumer.class);
    private IndexedPositionReader positionReader = new IndexedPositionReader(replayPositionBuffer);
    private ManageSessionEncoder logon = new ManageSessionEncoder();

    private ControlledFragmentHandler mockHandler = mock(ControlledFragmentHandler.class);
    private ErrorHandler errorHandler = mock(ErrorHandler.class);

    private ArchivingMediaDriver mediaDriver;
    private AeronArchive aeronArchive;

    private ExclusivePublication publication;
    private Subscription subscription;
    private RecordingIdStore recordingIdStore;

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
            recordingIdStore.outboundLookup());
    }

    private Aeron aeron()
    {
        return aeronArchive.context().aeron();
    }

    private ReplayQuery query;

    @Before
    public void setUp()
    {
        mediaDriver = TestFixtures.launchMediaDriver();
        aeronArchive = AeronArchive.connect();

        recordingIdStore = new RecordingIdStore(
            aeron(), CHANNEL, null, new YieldingIdleStrategy(), new YieldingIdleStrategy());

        aeronArchive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL);

        final Aeron aeron = aeron();
        publication = aeron.addExclusivePublication(CHANNEL, STREAM_ID);
        subscription = aeron.addSubscription(CHANNEL, STREAM_ID);

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
            aeronArchive,
            errorHandler);

        returnBuffer(indexBuffer, SESSION_ID);
        returnBuffer(ByteBuffer.allocate(16 * 1024), SESSION_ID_2);
        when(newBufferFactory.map(any(), anyInt())).thenReturn(indexBuffer);
    }

    @After
    public void teardown()
    {
        aeronArchive.stopRecording(CHANNEL, STREAM_ID);
        Exceptions.closeAll(replayIndex, aeronArchive);
        cleanupMediaDriver(mediaDriver);
    }

    @Test
    public void shouldReturnRecordsMatchingQuery()
    {
        indexExampleMessage();

        final int msgCount = query();

        verifyMappedFile(SESSION_ID, 1);
        verifyMessagesRead(1);
        assertEquals(1, msgCount);
    }

    @Test
    public void shouldReturnLongRecordsMatchingQuery()
    {
        final String testReqId = largeTestReqId();

        bufferContainsExampleMessage(true, SESSION_ID, SEQUENCE_NUMBER, SEQUENCE_INDEX, testReqId);
        publishBuffer();
        indexRecord(11);

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
        indexExampleMessage(SESSION_ID, endSequenceNumber, SEQUENCE_INDEX);

        final int msgCount = query(SEQUENCE_NUMBER, SEQUENCE_INDEX, endSequenceNumber, SEQUENCE_INDEX);

        verifyMessagesRead(2);
        assertEquals(2, msgCount);
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

        final int nextSequenceIndex = SEQUENCE_INDEX + 1;
        final int endSequenceNumber = 1;

        indexExampleMessage(SESSION_ID, endSequenceNumber, nextSequenceIndex);

        final int msgCount = query(SEQUENCE_NUMBER, SEQUENCE_INDEX, endSequenceNumber, nextSequenceIndex);

        verifyMessagesRead(2);
        assertEquals(2, msgCount);
    }

    @Test
    public void shouldNotStopIndexingWhenBufferFull()
    {
        indexExampleMessage();

        final int beginSequenceNumber = 1;
        final int endSequenceNumber = 1_000;

        IntStream.rangeClosed(beginSequenceNumber, endSequenceNumber).forEach(seqNum ->
            indexExampleMessage(SESSION_ID, seqNum, SEQUENCE_INDEX));

        final int msgCount = query(beginSequenceNumber, SEQUENCE_INDEX, endSequenceNumber, SEQUENCE_INDEX);

        assertEquals(totalMessages, msgCount);
        verifyMessagesRead(totalMessages);
    }

    @Test
    public void shouldUpdatePositionForIndexedRecord()
    {
        indexExampleMessage();

        positionReader.readLastPosition(positionConsumer);

        final int aeronSessionId = publication.sessionId();
        final long recordingId = recordingIdStore.outboundLookup().getRecordingId(aeronSessionId);

        verify(positionConsumer, times(1))
            .accept(aeronSessionId, recordingId, alignedEndPosition());
    }

    @Test
    public void shouldOnlyMapSessionFileOnce()
    {
        indexExampleMessage();

        indexExampleMessage();

        verifyMappedFile(SESSION_ID);
    }

    @Test
    public void shouldRecordIndexesForMultipleSessions()
    {
        indexExampleMessage();

        indexExampleMessage(SESSION_ID_2, SEQUENCE_NUMBER, SEQUENCE_INDEX);

        verifyMappedFile(SESSION_ID);
        verifyMappedFile(SESSION_ID_2);
    }

    @Test
    public void shouldIgnoreOtherMessageTypes()
    {
        bufferContainsLogon();

        publishBuffer();

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
        indexExampleMessage(SESSION_ID, SEQUENCE_NUMBER, SEQUENCE_INDEX);
    }

    private void verifyNoMessageRead()
    {
        verifyMessagesRead(never());
    }

    private void verifyMessagesRead(final int number)
    {
        verifyMessagesRead(times(number));
    }

    private void verifyMessagesRead(final VerificationMode times)
    {
        verify(mockHandler, times)
            .onFragment(any(), anyInt(), anyInt(), any());
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
        indexRecord(1);
    }

    private void indexRecord(final int fragmentsToRead)
    {
        int read = 0;
        while (read < fragmentsToRead)
        {
            read += subscription.poll(replayIndex, 1);
        }
    }

    private void indexExampleMessage(final long sessionId, final int sequenceNumber, final int sequenceIndex)
    {
        bufferContainsExampleMessage(true, sessionId, sequenceNumber, sequenceIndex);

        publishBuffer();

        indexRecord();
    }

    private void publishBuffer()
    {
        while (publication.offer(buffer, START, logEntryLength + PREFIX_LENGTH) <= 0)
        {
            Thread.yield();
        }
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
        final ReplayOperation operation = query.query(
            mockHandler, sessionId, beginSequenceNumber, beginSequenceIndex, endSequenceNumber, endSequenceIndex);
        while (!operation.attemptReplay())
        {
            Thread.yield();
        }
        return operation.replayedMessages();
    }
}
