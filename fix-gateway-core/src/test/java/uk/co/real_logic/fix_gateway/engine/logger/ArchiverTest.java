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
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.BREAK;
import static io.aeron.logbuffer.LogBufferDescriptor.computeTermIdFromPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.computeTermOffsetFromPosition;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.TestFixtures.cleanupDirectory;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.*;
import static uk.co.real_logic.fix_gateway.engine.logger.ArchiveDescriptor.alignTerm;
import static uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader.*;
import static uk.co.real_logic.fix_gateway.replication.ReservedValue.NO_FILTER;

@RunWith(Parameterized.class)
public class ArchiverTest
{

    private static final int TERM_LENGTH = 65536;
    private static final int STREAM_ID = 1;
    private static final int OFFSET_WITHIN_MESSAGE = 42;
    private static final int INITIAL_VALUE = 43;
    private static final int PATCH_VALUE = 44;
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:9999";

    @Parameters
    public static Collection<Object[]> data()
    {
        // TODO: enable more comprehensive testing in a CI environment
        return IntStream.of(1337/*, 129, 128, 4097*/)
                        .boxed()
                        .flatMap(size ->
                            Stream.of(
                                new UnsafeBuffer(ByteBuffer.allocateDirect(size)),
                                new UnsafeBuffer(new byte[size])))
                        .map(buffer -> new Object[]{buffer})
                        .collect(Collectors.toList());
    }

    private final BlockHandler blockHandler = mock(BlockHandler.class);
    private final ControlledFragmentHandler fragmentHandler = mock(ControlledFragmentHandler.class);
    private final ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
    private final ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);

    private final int size;
    private final UnsafeBuffer buffer;

    private LogDirectoryDescriptor logDirectoryDescriptor;
    private MediaDriver mediaDriver;
    private Aeron aeron;
    private Archiver archiver;
    private ArchiveReader archiveReader;
    private Publication publication;

    private int lastArchivedValue;
    private int work = 0;

    public ArchiverTest(final UnsafeBuffer buffer)
    {
        this.buffer = buffer;
        size = buffer.capacity();
    }

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver(TERM_LENGTH);
        aeron = Aeron.connect(new Aeron.Context().imageMapMode(READ_WRITE));

        final File logFileDir = new File(DEFAULT_LOG_FILE_DIR);
        if (logFileDir.exists())
        {
            IoUtil.delete(logFileDir, false);
        }

        final StreamIdentifier dataStream = new StreamIdentifier(CHANNEL, STREAM_ID);
        logDirectoryDescriptor = new LogDirectoryDescriptor(DEFAULT_LOG_FILE_DIR);
        final ArchiveMetaData metaData = new ArchiveMetaData(logDirectoryDescriptor);
        archiveReader = new ArchiveReader(
            metaData, DEFAULT_LOGGER_CACHE_NUM_SETS, DEFAULT_LOGGER_CACHE_SET_SIZE, dataStream);
        archiver = new Archiver(
            metaData, DEFAULT_LOGGER_CACHE_NUM_SETS, DEFAULT_LOGGER_CACHE_SET_SIZE, dataStream);

        publication = aeron.addPublication(CHANNEL, STREAM_ID);
        archiver.subscription(aeron.addSubscription(CHANNEL, STREAM_ID));
    }

    @Test
    public void shouldReadDataThatWasWritten()
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        assertReadsInitialValue(HEADER_LENGTH);
    }

    @Test
    public void shouldNotReadDataThatHasBeenCorrupted() throws IOException
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        corruptLogFile();

        final long position = read((long) HEADER_LENGTH);

        assertNothingRead(position, CORRUPT_LOG);
    }

    @Test
    public void shouldNotBlockReadDataThatHasBeenCorrupted() throws IOException
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        corruptLogFile();

        final boolean wasRead = readBlockTo((long) HEADER_LENGTH);

        assertNothingBlockRead(wasRead);
    }

    @Test
    public void shouldNotReadUpToDataThatHasBeenCorrupted() throws IOException
    {
        final long endPosition = writeAndArchiveBuffer(INITIAL_VALUE);

        corruptLogFile();

        final long readPosition = archiveReader.readUpTo(
            NO_FILTER, publication.sessionId(), (long) HEADER_LENGTH, endPosition, fragmentHandler);

        assertNothingRead(readPosition, CORRUPT_LOG);
    }

    private void corruptLogFile() throws IOException
    {
        final File file = logFiles().get(0);
        try (final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw"))
        {
            randomAccessFile.seek(HEADER_LENGTH);
            randomAccessFile.write(new byte[size]);
            randomAccessFile.getFD().sync();
        }
    }

    @Test
    public void shouldReadAfterException()
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        try
        {
            archiveReader.read(publication.sessionId(), (long)HEADER_LENGTH,
                (buffer, offset, length, header) ->
                {
                    throw new RuntimeException();
                });

            fail("continued despite exception");
        }
        catch (final RuntimeException ignore)
        {
        }

        assertReadsInitialValue(HEADER_LENGTH);
    }

    @Test
    public void shouldSupportRotatingFilesAtEndOfTerm()
    {
        archiveBeyondEndOfTerm();

        assertReadsValueAt(lastArchivedValue, TERM_LENGTH + HEADER_LENGTH);
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

        final long oneMessageIn = HEADER_LENGTH + alignTerm(size) + HEADER_LENGTH;

        readUpTo(0, oneMessageIn, times(1));
    }

    private void shouldReadFragmentsUpToAPosition(final int offsetIntoNextMessage)
    {
        final long twoMessagesIn = HEADER_LENGTH + lengthOfTwoMessages() + HEADER_LENGTH;

        readUpTo(offsetIntoNextMessage, twoMessagesIn, times(2));
    }

    private void readUpTo(
        final int offsetIntoNextMessage,
        final long expectedPosition,
        final VerificationMode times)
    {
        archiveBeyondEndOfTerm();

        final long begin = HEADER_LENGTH;
        final long end = begin + lengthOfTwoMessages() + offsetIntoNextMessage;
        final long res = archiveReader.readUpTo(NO_FILTER, publication.sessionId(), begin, end, fragmentHandler);

        verify(fragmentHandler, times).onFragment(bufferCaptor.capture(), offsetCaptor.capture(), anyInt(), any());

        assertEquals("Failed to return new position", expectedPosition, res);
    }

    @Test
    public void shouldNotReadDataForNotArchivedSession()
    {
        final long position = read((long)HEADER_LENGTH);

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

        assertCanBlockReadValueAt(HEADER_LENGTH);
    }

    @Test
    public void shouldBlockReadAfterException()
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        try
        {
            archiveReader.readBlock(publication.sessionId(), (long)HEADER_LENGTH, size,
                (buffer, offset, length, sessionId, termId) ->
                {
                    throw new RuntimeException();
                });

            fail("continued despite exception");
        }
        catch (final RuntimeException ignore)
        {
        }

        assertCanBlockReadValueAt(HEADER_LENGTH);
    }

    @Test
    public void shouldSupportRotatingFilesAtEndOfTermInBlockRead()
    {
        archiveBeyondEndOfTerm();

        assertCanBlockReadValueAt(lastArchivedValue, TERM_LENGTH + HEADER_LENGTH);
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
    public void shouldPatchCurrentTerm()
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        patchBuffer(0);

        assertReadsValueAt(PATCH_VALUE, HEADER_LENGTH);
    }

    @Test
    public void shouldPatchPreviousTerm()
    {
        archiveBeyondEndOfTerm();

        patchBuffer(0);

        assertReadsValueAt(PATCH_VALUE, HEADER_LENGTH);
    }

    @Test
    public void shouldPatchMissingTerm()
    {
        archiveBeyondEndOfTerm();

        removeLogFiles();

        patchBuffer(0);

        assertReadsValueAt(PATCH_VALUE, HEADER_LENGTH);
    }

    @Test
    public void shouldNotBeAbleToPatchTheFuture()
    {
        writeAndArchiveBuffer(INITIAL_VALUE);

        assertFalse("Patched the future", patchBuffer(TERM_LENGTH));
    }

    private int lengthOfTwoMessages()
    {
        return alignTerm(size) * 2 + HEADER_LENGTH;
    }

    private long read(final long position)
    {
        return archiveReader.read(publication.sessionId(), position, fragmentHandler);
    }

    private boolean readBlockTo(final long position)
    {
        return archiveReader.readBlock(publication.sessionId(), position, size, blockHandler);
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

    private boolean patchBuffer(final long position)
    {
        final int frameLength = HEADER_LENGTH + OFFSET_WITHIN_MESSAGE + SIZE_OF_INT;
        final int dataOffset = HEADER_LENGTH + OFFSET_WITHIN_MESSAGE;

        final int sessionId = publication.sessionId();
        final int streamId = publication.streamId();
        final int positionBitsToShift = numberOfTrailingZeros(publication.termBufferLength());
        final int initialTermId = publication.initialTermId();
        final int termId = computeTermIdFromPosition(position, positionBitsToShift, initialTermId);
        final int termOffset = computeTermOffsetFromPosition(position, positionBitsToShift);

        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight(new UnsafeBuffer(new byte[1024]));
        flyweight
            .sessionId(sessionId)
            .streamId(streamId)
            .termId(termId)
            .termOffset(termOffset)
            .frameLength(frameLength)
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags(DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA);

        flyweight.putInt(dataOffset, PATCH_VALUE);

        return archiver.patch(sessionId, flyweight, 0, frameLength);
    }

    private void assertPosition(final long endPosition)
    {
        assertEquals(endPosition, archiver.positionOf(publication.sessionId()));
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

    private void assertReadsInitialValue(final int position)
    {
        assertReadsValueAt(INITIAL_VALUE, position);
    }

    private void assertReadsValueAt(final int value, final long position)
    {
        final boolean hasRead = read(position) > 0;

        verify(fragmentHandler).onFragment(bufferCaptor.capture(), offsetCaptor.capture(), anyInt(), any());

        assertReadValue(value, position + HEADER_LENGTH, hasRead);
    }

    private void assertDataPublished(final long endPosition)
    {
        assertThat("Publication has failed an offer", endPosition, greaterThan((long) size));
    }

    private void assertCanBlockReadValueAt(final int position)
    {
        assertCanBlockReadValueAt(INITIAL_VALUE, position);
    }

    private void assertCanBlockReadValueAt(final int value, final long position)
    {
        final boolean hasRead = readBlockTo(position);

        verify(blockHandler).onBlock(
            bufferCaptor.capture(), offsetCaptor.capture(), eq(size), eq(publication.sessionId()), anyInt());

        assertReadValue(value, position, hasRead);
    }

    private void assertReadValue(final int value, final long positionOfMessageStart, final boolean hasRead)
    {
        assertEquals(value, bufferCaptor.getValue().getInt(offsetCaptor.getValue() + OFFSET_WITHIN_MESSAGE));
        assertTrue("Failed to read value at " + positionOfMessageStart, hasRead);
    }

    private long writeBuffer(final int value)
    {
        buffer.putInt(OFFSET_WITHIN_MESSAGE, value);

        long endPosition;
        do
        {
            endPosition = publication.offer(buffer, 0, size);
            LockSupport.parkNanos(100);
        }
        while (endPosition < 0);

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

    @After
    public void tearDown()
    {
        CloseHelper.close(archiveReader);
        archiver.onClose();
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
        cleanupDirectory(mediaDriver);

        System.gc();
    }
}
