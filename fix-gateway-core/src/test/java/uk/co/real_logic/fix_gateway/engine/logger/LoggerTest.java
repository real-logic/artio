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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.BlockHandler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;
import uk.co.real_logic.fix_gateway.protocol.Streams;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.locks.LockSupport;

import static java.lang.Integer.numberOfTrailingZeros;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static io.aeron.logbuffer.LogBufferDescriptor.computeTermIdFromPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.computeTermOffsetFromPosition;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.findNextPositivePowerOfTwo;
import static uk.co.real_logic.fix_gateway.TestFixtures.cleanupDirectory;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader.*;

@RunWith(Parameterized.class)
public class LoggerTest
{

    public static final int SIZE = 2 * 1024;
    public static final int TERM_LENGTH = findNextPositivePowerOfTwo(SIZE * 32);
    public static final int STREAM_ID = 1;
    public static final int OFFSET_WITHIN_MESSAGE = 42;
    public static final int INITIAL_VALUE = 43;
    public static final int PATCH_VALUE = 44;
    public static final String CHANNEL = "udp://localhost:9999";

    @Parameters
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[][]{
            {new UnsafeBuffer(ByteBuffer.allocateDirect(SIZE))},
            {new UnsafeBuffer(new byte[SIZE])},
        });
    }

    private final BlockHandler blockHandler = mock(BlockHandler.class);
    private final FragmentHandler fragmentHandler = mock(FragmentHandler.class);
    private final ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
    private final ArgumentCaptor<Integer> offsetCaptor = ArgumentCaptor.forClass(Integer.class);

    private final UnsafeBuffer buffer;

    private EngineConfiguration configuration;
    private MediaDriver mediaDriver;
    private Aeron aeron;
    private Logger logger;
    private Archiver archiver;
    private ArchiveReader archiveReader;
    private Publication publication;

    private int work = 0;

    public LoggerTest(final UnsafeBuffer buffer)
    {
        this.buffer = buffer;
    }

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver(TERM_LENGTH);
        aeron = Aeron.connect(new Aeron.Context());
        final Streams outboundStreams = new Streams(
            CHANNEL, aeron, mock(AtomicCounter.class), STREAM_ID, mock(NanoClock.class), 12000);

        configuration = new EngineConfiguration()
            .logInboundMessages(false)
            .aeronChannel(CHANNEL);

        final File dir = new File(configuration.logFileDir());
        if (dir.exists())
        {
            IoUtil.delete(dir, false);
        }

        configuration.conclude();

        logger = new Logger(
            configuration, null, outboundStreams, Throwable::printStackTrace, null,
            mock(SequenceNumberIndexWriter.class), mock(SequenceNumberIndexWriter.class)
        );

        logger.initArchival();
        archiver = logger.archivers().get(0);
        archiveReader = logger.outboundArchiveReader();
        publication = outboundStreams.dataPublication();
    }

    @Test
    public void shouldReadDataThatWasWritten()
    {
        writeAndArchiveBuffer();

        assertReadsInitialValue(HEADER_LENGTH);
    }

    @Test
    public void shouldReadAfterException()
    {
        writeAndArchiveBuffer();

        try
        {
            archiveReader.read(publication.sessionId(), (long) HEADER_LENGTH,
                (buffer, offset, length, header) -> {
                    throw new RuntimeException();
                });

            fail("continued despite exception");
        }
        catch (RuntimeException e)
        {
            // Deliberately Blank
        }

        assertReadsInitialValue(HEADER_LENGTH);
    }

    @Test
    public void shouldSupportRotatingFilesAtEndOfTerm()
    {
        archiveBeyondEndOfTerm();

        assertReadsInitialValue(TERM_LENGTH + HEADER_LENGTH);
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

    private void shouldReadFragmentsUpToAPosition(final int offsetIntoNextMessage)
    {
        archiveBeyondEndOfTerm();

        final long begin = HEADER_LENGTH;
        final int lengthOfTwoMessages = SIZE * 2 + HEADER_LENGTH;
        final long end = begin + lengthOfTwoMessages + offsetIntoNextMessage;
        final long res = archiveReader.readUpTo(publication.sessionId(), begin, end, fragmentHandler);

        verify(fragmentHandler, times(2)).onFragment(bufferCaptor.capture(), offsetCaptor.capture(), anyInt(), any());

        final long twoMessagesIn = begin + lengthOfTwoMessages + HEADER_LENGTH;
        assertEquals("Failed to return new position", twoMessagesIn, res);
    }

    @Test
    public void shouldNotReadDataForNotArchivedSession()
    {
        final long position = readTo((long) HEADER_LENGTH);

        assertNothingRead(position, UNKNOWN_SESSION);
    }

    @Test
    public void shouldNotReadDataForNotArchivedTerm()
    {
        writeAndArchiveBuffer();

        final long position = readTo(TERM_LENGTH + HEADER_LENGTH);

        assertNothingRead(position, UNKNOWN_TERM);
    }

    @Test
    public void shouldNotReadNotArchivedDataInCurrentTerm()
    {
        final long endPosition = writeAndArchiveBuffer();

        final long position = readTo(endPosition * 2);

        assertNothingRead(position, NO_MESSAGE);
    }

    @Test
    public void shouldBlockReadDataThatWasWritten()
    {
        writeAndArchiveBuffer();

        assertCanBlockReadValueAt(HEADER_LENGTH);
    }

    @Test
    public void shouldBlockReadAfterException()
    {
        writeAndArchiveBuffer();

        try
        {
            archiveReader.readBlock(publication.sessionId(), (long) HEADER_LENGTH, SIZE,
                (buffer, offset, length, sessionId, termId) -> {
                    throw new RuntimeException();
                });

            fail("continued despite exception");
        }
        catch (RuntimeException e)
        {
            // Deliberately Blank
        }

        assertCanBlockReadValueAt(HEADER_LENGTH);
    }

    @Test
    public void shouldSupportRotatingFilesAtEndOfTermInBlockRead()
    {
        archiveBeyondEndOfTerm();

        assertCanBlockReadValueAt(TERM_LENGTH + HEADER_LENGTH);
    }

    @Test
    public void shouldNotBlockReadDataForNotArchivedSession()
    {
        final boolean wasRead = readBlockTo((long) HEADER_LENGTH);

        assertNothingBlockRead(wasRead);
    }

    @Test
    public void shouldNotBlockReadDataForNotArchivedTerm()
    {
        writeAndArchiveBuffer();

        final boolean wasRead = readBlockTo(TERM_LENGTH + HEADER_LENGTH);

        assertNothingBlockRead(wasRead);
    }

    @Test
    public void shouldUpdatePosition()
    {
        final long endPosition = writeAndArchiveBuffer();

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
        writeAndArchiveBuffer();

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
        writeAndArchiveBuffer();

        assertFalse("Patched the future", patchBuffer(TERM_LENGTH));
    }

    private long readTo(final long position)
    {
        return archiveReader.read(publication.sessionId(), position, fragmentHandler);
    }

    private boolean readBlockTo(final long position)
    {
        return archiveReader.readBlock(publication.sessionId(), position, SIZE, blockHandler);
    }

    private void removeLogFiles()
    {
        logger
            .directoryDescriptor()
            .listLogFiles(new StreamIdentifier(CHANNEL, STREAM_ID))
            .forEach(File::delete);
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

        do
        {
            endPosition = writeAndArchiveBuffer();
        }
        while (endPosition <= TERM_LENGTH);

        return endPosition;
    }

    private long writeAndArchiveBuffer()
    {
        final long endPosition = writeBuffer(INITIAL_VALUE);

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
        final boolean hasRead = readTo(position) > 0;

        verify(fragmentHandler).onFragment(bufferCaptor.capture(), offsetCaptor.capture(), anyInt(), any());

        assertReadValue(value, position + HEADER_LENGTH, hasRead);
    }

    private void assertDataPublished(final long endPosition)
    {
        assertThat("Publication has failed an offer", endPosition, greaterThan((long) SIZE));
    }

    private void assertCanBlockReadValueAt(final int position)
    {
        assertCanBlockReadValueAt(INITIAL_VALUE, position);
    }

    private void assertCanBlockReadValueAt(final int value, final long position)
    {
        final boolean hasRead = readBlockTo(position);

        verify(blockHandler).onBlock(
            bufferCaptor.capture(), offsetCaptor.capture(), eq(SIZE), eq(publication.sessionId()), anyInt());

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
            endPosition = publication.offer(buffer, 0, SIZE);
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
        } while (work < endPosition);
    }

    @After
    public void tearDown()
    {
        CloseHelper.close(logger);
        CloseHelper.close(configuration);
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
        cleanupDirectory(mediaDriver);

        System.gc();
    }

}
