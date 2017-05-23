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
package uk.co.real_logic.fix_gateway.engine.logger;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BlockHandler;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.engine.CompletionPosition;
import uk.co.real_logic.fix_gateway.replication.ReservedValue;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.aeron.driver.Configuration.MTU_LENGTH;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.BREAK;
import static io.aeron.logbuffer.LogBufferDescriptor.computeTermIdFromPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.computeTermOffsetFromPosition;
import static io.aeron.protocol.DataHeaderFlyweight.END_FLAG;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.lang.Integer.min;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.DEFAULT_NAME_PREFIX;
import static uk.co.real_logic.fix_gateway.TestFixtures.cleanupMediaDriver;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.DEFAULT_LOGGER_CACHE_NUM_SETS;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.DEFAULT_LOGGER_CACHE_SET_SIZE;
import static uk.co.real_logic.fix_gateway.engine.logger.ArchiveDescriptor.alignTerm;
import static uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader.*;
import static uk.co.real_logic.fix_gateway.replication.ReservedValue.NO_FILTER;

@RunWith(Parameterized.class)
public class ArchiverTest
{
    private static final int TERM_LENGTH = 131072;
    private static final int STREAM_ID = 1;
    private static final int OFFSET_WITHIN_MESSAGE = 42;
    private static final int INITIAL_VALUE = 43;
    private static final int PATCH_VALUE = 44;
    private static final int RESERVED_VALUE = 1;
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:9999";
    private static final String LOG_FILE_DIR = tmpLogsDirName() + "ArchiverTest-logs";

    @Parameters(name = "{0},{1}")
    public static Collection<Object[]> data()
    {
        // TODO: enable more comprehensive testing in a CI environment
        return IntStream
            .of(1337, 6003/*, 129, 128, 4097*/)
            .boxed()
            .flatMap((size) ->
                Stream.of(
                    //new UnsafeBuffer(ByteBuffer.allocateDirect(size)),
                    new UnsafeBuffer(new byte[size])))
            .map(buffer -> new Object[]{ buffer.capacity(), buffer })
            .collect(Collectors.toList());
    }

    private final BlockHandler blockHandler = mock(BlockHandler.class);
    private final ControlledFragmentHandler fragmentHandler = mock(ControlledFragmentHandler.class);
    private final ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
    private final ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);
    private final ArgumentCaptor<Integer> lengthCaptor = ArgumentCaptor.forClass(Integer.class);
    private final CompletionPosition completionPosition = mock(CompletionPosition.class);
    private final Long2LongHashMap completedPositions = new Long2LongHashMap(CompletionPosition.MISSING_VALUE);

    private final int size;
    private final int endOfFirstMessage;
    private final double fragments;
    private final UnsafeBuffer buffer;
    private final UnsafeBuffer writeBuffer;

    private LogDirectoryDescriptor logDirectoryDescriptor;
    private MediaDriver mediaDriver;
    private Aeron aeron;
    private Archiver archiver;
    private ArchiveReader archiveReader;
    private ArchiveReader filteredArchiveReader;
    private Publication publication;

    private int lastArchivedValue;
    private int work = 0;

    public ArchiverTest(final int size, final UnsafeBuffer buffer)
    {
        this.buffer = buffer;
        this.size = size;
        writeBuffer = new UnsafeBuffer(new byte[size]);
        fragments = (double) size / MTU_LENGTH;
        endOfFirstMessage = HEADER_LENGTH + alignTerm(size);
    }

    @Before
    public void setUp()
    {
        when(completionPosition.hasCompleted()).thenReturn(true);
        when(completionPosition.positions()).thenReturn(completedPositions);

        deleteLogFileDir();

        mediaDriver = launchMediaDriver(TERM_LENGTH);
        aeron = Aeron.connect(new Aeron.Context().imageMapMode(READ_WRITE));

        final StreamIdentifier dataStream = new StreamIdentifier(CHANNEL, STREAM_ID);
        logDirectoryDescriptor = new LogDirectoryDescriptor(LOG_FILE_DIR);
        final ArchiveMetaData metaData = new ArchiveMetaData(logDirectoryDescriptor);
        archiveReader = new ArchiveReader(
            metaData, DEFAULT_LOGGER_CACHE_NUM_SETS, DEFAULT_LOGGER_CACHE_SET_SIZE, dataStream, NO_FILTER);
        filteredArchiveReader = new ArchiveReader(
            metaData, DEFAULT_LOGGER_CACHE_NUM_SETS, DEFAULT_LOGGER_CACHE_SET_SIZE, dataStream, RESERVED_VALUE);
        archiver = new Archiver(
            metaData, DEFAULT_LOGGER_CACHE_NUM_SETS, DEFAULT_LOGGER_CACHE_SET_SIZE, dataStream, DEFAULT_NAME_PREFIX,
            completionPosition);

        publication = aeron.addPublication(CHANNEL, STREAM_ID);
        archiver.subscription(aeron.addSubscription(CHANNEL, STREAM_ID));
    }

    @After
    public void tearDown()
    {
        CloseHelper.close(archiveReader);
        archiver.onClose();
        CloseHelper.close(filteredArchiveReader);

        CloseHelper.close(aeron);
        cleanupMediaDriver(mediaDriver);

        deleteLogFileDir();
    }

    private void deleteLogFileDir()
    {
        final File logFileDir = new File(LOG_FILE_DIR);
        if (logFileDir.exists())
        {
            IoUtil.delete(logFileDir, false);
        }
    }

    public static String tmpLogsDirName()
    {
        String dirName = IoUtil.tmpDirName();

        if ("Linux".equalsIgnoreCase(System.getProperty("os.name")))
        {
            final File devShmDir = new File("/dev/shm");

            if (devShmDir.exists())
            {
                dirName = "/dev/shm/";
            }
        }

        return dirName;
    }

    @Test
    public void shouldReadDataThatWasWritten()
    {
        final long endPosition = writeAndArchiveBuffer(INITIAL_VALUE);

        assertReadsInitialValue(HEADER_LENGTH, endPosition);
    }

    @Test
    public void shouldReadFilteredDataThatWasWritten()
    {
        final long endPosition = writeAndArchiveBuffer(INITIAL_VALUE, RESERVED_VALUE);

        assertReadsValueAt(INITIAL_VALUE, (long)HEADER_LENGTH, filteredArchiveReader, endPosition);
    }

    @Test
    public void shouldFilteredDataThatWasWritten()
    {
        writeAndArchiveBuffer(INITIAL_VALUE, 0);

        final long position = read((long)HEADER_LENGTH, filteredArchiveReader);

        assertNothingRead(position, NO_MESSAGE);
    }

    @Test
    public void shouldNotReadDataThatHasBeenCorrupted() throws IOException
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        corruptLogFile();

        final long position = read((long)HEADER_LENGTH);

        assertNothingRead(position, CORRUPT_LOG);
    }

    @Test
    public void shouldNotBlockReadDataThatHasBeenCorrupted() throws IOException
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        corruptLogFile();

        final boolean wasRead = readBlockTo((long)HEADER_LENGTH);

        assertNothingBlockRead(wasRead);
    }

    @Test
    public void shouldNotReadUpToDataThatHasBeenCorrupted() throws IOException
    {
        final long endPosition = writeAndArchiveBuffer(INITIAL_VALUE);

        corruptLogFile();

        final long readPosition = archiveReader.readUpTo(
            sessionId(), (long)HEADER_LENGTH, endPosition, fragmentHandler);

        assertNothingRead(readPosition, CORRUPT_LOG);
    }

    private int sessionId()
    {
        return publication.sessionId();
    }

    private void corruptLogFile() throws IOException
    {
        final File file = logFiles().get(0);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw"))
        {
            randomAccessFile.seek(HEADER_LENGTH);
            randomAccessFile.write(new byte[size]);
            randomAccessFile.getFD().sync();
        }
    }

    @Test
    public void shouldReadAfterException()
    {
        final long endPosition = writeAndArchiveBuffer(INITIAL_VALUE);

        try
        {
            archiveReader.read(sessionId(), (long)HEADER_LENGTH,
                (buffer, offset, length, header) ->
                {
                    throw new RuntimeException();
                });

            fail("continued despite exception");
        }
        catch (final RuntimeException ignore)
        {
        }

        assertReadsInitialValue(HEADER_LENGTH, endPosition);
    }

    @Test
    public void shouldSupportRotatingFilesAtEndOfTerm()
    {
        final long endPosition = archiveBeyondEndOfTerm();

        assertReadsValueAt(lastArchivedValue, TERM_LENGTH + HEADER_LENGTH, endPosition);
    }

    @Test
    public void shouldReadFragmentsUpToAPosition()
    {
        shouldReadFragmentsUpToAPosition(0);
    }

    @Test
    public void shouldNotReadBeyondEndOfFragment()
    {
        shouldReadFragmentsUpToAPosition(30);
    }

    @Test
    public void shouldNotReadAfterAbort()
    {
        when(fragmentHandler.onFragment(any(), anyInt(), anyInt(), any()))
            .thenReturn(ABORT);

        readUpTo(0, HEADER_LENGTH, times(1));
    }

    @Test
    public void shouldStopReadingAfterBreak()
    {
        when(fragmentHandler.onFragment(any(), anyInt(), anyInt(), any()))
            .thenReturn(BREAK);

        final int maxFragmentSize = min(alignTerm(size) + HEADER_LENGTH, MTU_LENGTH);
        final long oneMessageIn = HEADER_LENGTH + maxFragmentSize;

        readUpTo(0, oneMessageIn, times(1));
    }

    private void shouldReadFragmentsUpToAPosition(final int offsetIntoNextMessage)
    {
        final long endOfFirstMessage = HEADER_LENGTH + alignTerm(size);
        final long twoMessagesIn =
            endOfFirstMessage + min(alignTerm(size), MTU_LENGTH) + HEADER_LENGTH + HEADER_LENGTH;

        readUpTo(offsetIntoNextMessage, twoMessagesIn, fragmentScaledTimes2());
    }

    private VerificationMode fragmentScaledTimes2()
    {
        if (fragments > 1)
        {
            // If the message is over MTU length then you need to scale the number of callbacks up by that ratio
            return times((int) Math.ceil((2 * fragments)));
        }
        else
        {
            return times(2);
        }
    }

    private void readUpTo(
        final int offsetIntoNextMessage,
        final long expectedPosition,
        final VerificationMode times)
    {
        archiveBeyondEndOfTerm();

        final long begin = HEADER_LENGTH;
        final long end = begin + lengthOfTwoMessages() + offsetIntoNextMessage;
        final long resultPosition = archiveReader.readUpTo(sessionId(), begin, end, fragmentHandler);

        verify(fragmentHandler, times).onFragment(bufferCaptor.capture(), offsetCaptor.capture(), anyInt(), any());

        assertEquals("Failed to return new position", expectedPosition, resultPosition);
    }

    @Test
    public void shouldNotReadDataForNotArchivedSession()
    {
        final long position = read(HEADER_LENGTH);

        assertNothingRead(position, UNKNOWN_SESSION);
    }

    @Test
    public void shouldNotReadDataForNotArchivedTerm()
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        final long position = read(TERM_LENGTH + HEADER_LENGTH);

        assertNothingRead(position, UNKNOWN_TERM);
    }

    @Test
    public void shouldNotReadNotArchivedDataInCurrentTerm()
    {
        final long endPosition = writeAndArchiveBuffer(INITIAL_VALUE);

        final long position = read(endPosition * 2);

        assertNothingRead(position, NO_MESSAGE);
    }

    @Test
    public void shouldBlockReadDataThatWasWritten()
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        assertBlockReadsValueAt(HEADER_LENGTH);
    }

    public void shouldBlockReadFilteredDataThatWasWritten()
    {
        writeAndArchiveBuffer(INITIAL_VALUE, RESERVED_VALUE);

        assertBlockReadsValueAt(INITIAL_VALUE, HEADER_LENGTH, filteredArchiveReader);
    }

    @Test
    public void shouldBlockFilteredDataThatWasWritten()
    {
        writeAndArchiveBuffer(INITIAL_VALUE, 0);

        final boolean wasRead = readBlockTo(HEADER_LENGTH, filteredArchiveReader);

        assertNothingBlockRead(wasRead);
    }

    @Test
    public void shouldBlockReadAfterException()
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        try
        {
            archiveReader.readBlock(
                sessionId(),
                (long)HEADER_LENGTH, size,
                (buffer, offset, length, sessionId, termId) ->
                {
                    throw new RuntimeException();
                });

            fail("continued despite exception");
        }
        catch (final RuntimeException ignore)
        {
        }

        assertBlockReadsValueAt(HEADER_LENGTH);
    }

    @Test
    public void shouldSupportRotatingFilesAtEndOfTermInBlockRead()
    {
        archiveBeyondEndOfTerm();

        assertBlockReadsValueAt(lastArchivedValue, TERM_LENGTH + HEADER_LENGTH, archiveReader);
    }

    @Test
    public void shouldNotBlockReadDataForNotArchivedSession()
    {
        final boolean wasRead = readBlockTo((long)HEADER_LENGTH);

        assertNothingBlockRead(wasRead);
    }

    @Test
    public void shouldNotBlockReadDataForNotArchivedTerm()
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        final boolean wasRead = readBlockTo(TERM_LENGTH + HEADER_LENGTH);

        assertNothingBlockRead(wasRead);
    }

    @Test
    public void shouldUpdatePosition()
    {
        final long endPosition = writeAndArchiveBuffer(INITIAL_VALUE);

        assertPosition(endPosition);
    }

    @Test
    public void shouldUpdatePositionDuringRotation()
    {
        final long position = archiveBeyondEndOfTerm();

        assertPosition(position);
    }

    @Test
    public void shouldPatchCurrentTermFromArray()
    {
        shouldPatchCurrentTerm(true);
    }

    @Test
    public void shouldPatchCurrentTermFromBuffer()
    {
        shouldPatchCurrentTerm(false);
    }

    public void shouldPatchCurrentTerm(final boolean isArray)
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        patchBuffer(0, isArray);

        assertReadsValueAt(PATCH_VALUE, HEADER_LENGTH, endOfFirstMessage);
    }

    @Test
    public void shouldPatchWrapAroundTermFromArray()
    {
        shouldPatchWrapAroundTerm(true);
    }

    @Test
    public void shouldPatchWrapAroundTermFromBuffer()
    {
        shouldPatchWrapAroundTerm(false);
    }

    private void shouldPatchWrapAroundTerm(final boolean isArray)
    {
        long endPosition = archiveBeyondEndOfTerm();

        patchBuffer(TERM_LENGTH, isArray);

        if (size > publication.maxPayloadLength())
        {
            endPosition -= HEADER_LENGTH;
        }
        assertReadsValueAt(PATCH_VALUE, TERM_LENGTH + HEADER_LENGTH, endPosition);
    }

    @Test
    public void shouldPatchPreviousTermFromArray()
    {
        shouldPatchPreviousTerm(true);
    }

    @Test
    public void shouldPatchPreviousTermFromBuffer()
    {
        shouldPatchPreviousTerm(false);
    }

    public void shouldPatchPreviousTerm(final boolean isArray)
    {
        archiveBeyondEndOfTerm();

        patchBuffer(0, isArray);

        assertReadsValueAt(PATCH_VALUE, HEADER_LENGTH, endOfFirstMessage);
    }

    @Test
    public void shouldPatchMissingTermFromArray()
    {
        shouldPatchMissingTerm(true);
    }

    @Test
    public void shouldPatchMissingTermFromBuffer()
    {
        shouldPatchMissingTerm(false);
    }

    public void shouldPatchMissingTerm(final boolean isArray)
    {
        archiveBeyondEndOfTerm();

        removeLogFiles();

        patchBuffer(0, isArray);

        assertReadsValueAt(PATCH_VALUE, HEADER_LENGTH, endOfFirstMessage);
    }

    @Test
    public void shouldNotBeAbleToPatchTheFutureFromArray()
    {
        shouldNotBeAbleToPatchTheFuture(true);
    }

    @Test
    public void shouldNotBeAbleToPatchTheFutureFromBuffer()
    {
        shouldNotBeAbleToPatchTheFuture(false);
    }

    public void shouldNotBeAbleToPatchTheFuture(final boolean isArray)
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        assertFalse("Patched the future", patchBuffer(TERM_LENGTH, isArray));
    }

    @Test
    public void shouldNotPatchEmptyBuffer()
    {
        // Patches could be empty - leader will send empty patch if the block is missing or corrupted.
        // This should not cause an exception, reproduces #97
        writeAndArchiveBuffer(INITIAL_VALUE);

        final UnsafeBuffer emptyBuffer = new UnsafeBuffer(new byte[0]);
        assertFalse(archiver.patch(sessionId(), emptyBuffer, 0, 0));
    }

    @Test
    public void shouldArchiveToCompletedPositionWhenClosed()
    {
        final long endOfFirstBuffer = writeAndArchiveBuffer(INITIAL_VALUE);

        final long endOfSecondBuffer = writeBuffer(PATCH_VALUE);

        completedPositions.put(sessionId(), endOfSecondBuffer);

        archiver.onClose();

        final long startOfSecondBuffer = endOfFirstBuffer + HEADER_LENGTH;
        assertReadsValueAt(PATCH_VALUE, startOfSecondBuffer, endOfSecondBuffer);
    }

    private int lengthOfTwoMessages()
    {
        return alignTerm(size) * 2 + HEADER_LENGTH;
    }

    private long read(final long position)
    {
        return read(position, archiveReader);
    }

    private long read(final long position, final ArchiveReader archiveReader)
    {
        return archiveReader.read(sessionId(), position, fragmentHandler);
    }

    private boolean readBlockTo(final long position)
    {
        return readBlockTo(position, archiveReader);
    }

    private boolean readBlockTo(final long position, final ArchiveReader archiveReader)
    {
        return archiveReader.readBlock(sessionId(), position, size, blockHandler);
    }

    private void removeLogFiles()
    {
        logFiles().forEach(File::delete);
    }

    private List<File> logFiles()
    {
        return logDirectoryDescriptor.listLogFiles(new StreamIdentifier(CHANNEL, STREAM_ID));
    }

    private void assertNothingRead(final long position, final long expectedReason)
    {
        assertEquals("Claimed to read missing data", expectedReason, position);
        verify(fragmentHandler, never()).onFragment(any(), anyInt(), anyInt(), any());
    }

    private void assertNothingBlockRead(final boolean wasRead)
    {
        assertFalse("Claimed to read missing data", wasRead);
        verify(blockHandler, never()).onBlock(any(), anyInt(), anyInt(), anyInt(), anyInt());
    }

    private boolean patchBuffer(final long position, final boolean wrapsArray)
    {
        final int frameLength = HEADER_LENGTH + size;
        final int dataOffset = HEADER_LENGTH + OFFSET_WITHIN_MESSAGE;
        final int maxMessageLength = publication.maxMessageLength();
        final boolean fragment = frameLength > maxMessageLength;

        final int sessionId = sessionId();
        final int streamId = publication.streamId();
        final int positionBitsToShift = numberOfTrailingZeros(publication.termBufferLength());
        final int initialTermId = publication.initialTermId();
        final int termId = computeTermIdFromPosition(position, positionBitsToShift, initialTermId);
        final int termOffset = computeTermOffsetFromPosition(position, positionBitsToShift);

        final int wrapAdjustment = 4;
        final UnsafeBuffer buffer = wrapsArray ?
            new UnsafeBuffer(new byte[frameLength + wrapAdjustment], wrapAdjustment, frameLength) :
            new UnsafeBuffer(ByteBuffer.allocateDirect(frameLength + wrapAdjustment), wrapAdjustment, frameLength);
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(buffer);
        flyweight
            .sessionId(sessionId)
            .streamId(streamId)
            .termId(termId)
            .termOffset(termOffset)
            .frameLength(fragment ? maxMessageLength : frameLength)
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags(fragment ? DataHeaderFlyweight.BEGIN_FLAG : DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA);

        flyweight.putInt(dataOffset, PATCH_VALUE);

        if (fragment)
        {
            final int remaining = frameLength - maxMessageLength;
            flyweight.wrap(buffer, maxMessageLength, remaining);
            flyweight
                .sessionId(sessionId)
                .streamId(streamId)
                .termId(termId)
                .termOffset(termOffset + maxMessageLength)
                .frameLength(remaining)
                .version(HeaderFlyweight.CURRENT_VERSION)
                .flags(END_FLAG)
                .headerType(HeaderFlyweight.HDR_TYPE_DATA);

            flyweight.putInt(maxMessageLength + dataOffset, PATCH_VALUE);
        }

        return archiver.patch(sessionId, flyweight, 0, frameLength);
    }

    private void assertPosition(final long endPosition)
    {
        assertEquals(endPosition, archiver.positionOf(sessionId()));
    }

    private long archiveBeyondEndOfTerm()
    {
        long endPosition;

        int value = INITIAL_VALUE - 1;
        do
        {
            value++;
            endPosition = writeAndArchiveBuffer(value);
        }
        while (endPosition <= TERM_LENGTH);

        this.lastArchivedValue = value;

        return endPosition;
    }

    private long writeAndArchiveBuffer(final int value)
    {
        final long endPosition = writeBuffer(value);

        assertDataPublished(endPosition);

        archiveUpTo(endPosition);

        return endPosition;
    }

    private long writeAndArchiveBuffer(final int value, final int clusterStreamId)
    {
        final long reservedValue = ReservedValue.ofClusterStreamId(clusterStreamId);
        writeBuffer.putInt(OFFSET_WITHIN_MESSAGE, value);

        long endPosition;
        do
        {
            endPosition = publication.offer(
                writeBuffer, 0, size, (termBuffer, termOffset, frameLength) -> reservedValue);

            Thread.yield();
        }
        while (endPosition < 0);

        assertDataPublished(endPosition);

        archiveUpTo(endPosition);

        return endPosition;
    }

    private void assertReadsInitialValue(final int position, final long endPosition)
    {
        assertReadsValueAt(INITIAL_VALUE, position, endPosition);
    }

    private void assertReadsValueAt(final int value, final long position, final long endPosition)
    {
        assertReadsValueAt(value, position, archiveReader, endPosition);
    }

    private void assertReadsValueAt(
        final int value,
        final long position,
        final ArchiveReader archiveReader,
        final long expectedEndPosition)
    {
        final long positionOfMessageStart = position + HEADER_LENGTH;
        final boolean hasRead = read(position, archiveReader) > 0;

        verify(fragmentHandler, atLeastOnce()).onFragment(
            bufferCaptor.capture(), offsetCaptor.capture(), lengthCaptor.capture(), any());

        assertReadValue(value, positionOfMessageStart, hasRead);

        final long endPosition = lengthCaptor
            .getAllValues()
            .stream()
            .reduce(
                position - HEADER_LENGTH,
                (acc, len) -> ArchiveDescriptor.nextTerm(acc + HEADER_LENGTH, len),
                Long::sum);
        assertEquals(expectedEndPosition, endPosition);
    }

    private void assertDataPublished(final long endPosition)
    {
        assertThat("Publication has failed an offer", endPosition, greaterThan((long)size));
    }

    private void assertBlockReadsValueAt(final int position)
    {
        assertBlockReadsValueAt(INITIAL_VALUE, position, archiveReader);
    }

    private void assertBlockReadsValueAt(final int value, final long position, final ArchiveReader archiveReader)
    {
        final boolean hasRead = readBlockTo(position, archiveReader);

        verify(blockHandler).onBlock(
            bufferCaptor.capture(), offsetCaptor.capture(), eq(size), eq(sessionId()), anyInt());

        assertReadValue(value, position, hasRead);
    }

    private void assertReadValue(final int value, final long positionOfMessageStart, final boolean hasRead)
    {
        final DirectBuffer buffer = bufferCaptor.getAllValues().get(0);
        final int offset = offsetCaptor.getAllValues().get(0);

        assertEquals(value, buffer.getInt(offset + OFFSET_WITHIN_MESSAGE));
        assertTrue("Failed to read value at " + positionOfMessageStart, hasRead);
    }

    private long writeBuffer(final int value)
    {
        buffer.putInt(OFFSET_WITHIN_MESSAGE, value);

        long endPosition;
        while (true)
        {
            endPosition = publication.offer(buffer, 0, size);
            if (endPosition >= 0)
            {
                break;
            }

            Thread.yield();
        }

        return endPosition;
    }

    private void archiveUpTo(final long endPosition)
    {
        do
        {
            work += archiver.doWork();
        }
        while (work < endPosition);
    }
}
