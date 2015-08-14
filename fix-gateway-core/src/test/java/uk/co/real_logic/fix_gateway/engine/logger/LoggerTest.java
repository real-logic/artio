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
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.EngineConfiguration;
import uk.co.real_logic.fix_gateway.streams.ReplicatedStream;

import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.aeron.driver.Configuration.TERM_BUFFER_LENGTH_PROP_NAME;
import static uk.co.real_logic.agrona.BitUtil.findNextPositivePowerOfTwo;
import static uk.co.real_logic.agrona.CloseHelper.quietClose;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;

public class LoggerTest
{
    public static final int SIZE = 64 * 1024;
    public static final int TERM_LENGTH = findNextPositivePowerOfTwo(SIZE * 8);
    public static final int STREAM_ID = 1;
    public static final int OFFSET = 42;
    public static final int VALUE = 43;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[SIZE]);

    private MediaDriver mediaDriver;
    private Aeron aeron;
    private ReplicatedStream inboundStreams;
    private Logger logger;
    private Archiver archiver;
    private ArchiveReader archiveReader;
    private Publication publication;

    private int work = 0;

    @Before
    public void setUp()
    {
        System.setProperty(TERM_BUFFER_LENGTH_PROP_NAME, String.valueOf(TERM_LENGTH));

        mediaDriver = launchMediaDriver();
        aeron = Aeron.connect(new Aeron.Context());
        inboundStreams = new ReplicatedStream("udp://localhost:9999", aeron, mock(AtomicCounter.class), STREAM_ID);

        logger = new Logger(
            new EngineConfiguration().logOutboundMessages(false), inboundStreams, null, Throwable::printStackTrace);

        logger.initArchival();
        archiver = logger.archiver();
        archiveReader = logger.archiveReader();
        publication = inboundStreams.dataPublication();

        buffer.putInt(OFFSET, VALUE);
    }

    @Test
    public void shouldReadDataThatWasWritten()
    {
        final long endPosition = writeBuffer();

        assertThat("Publication has failed an offer", endPosition, greaterThan((long) SIZE));

        archiveUpTo(endPosition);

        assertCanReadValueAt(DataHeaderFlyweight.HEADER_LENGTH);
    }

    @Test
    public void shouldSupportRotatingFilesAtEndOfTerm()
    {
        long endPosition;

        do
        {
            endPosition = writeBuffer();

            assertThat("Publication has failed an offer", endPosition, greaterThan((long) SIZE));

            archiveUpTo(endPosition);
        }
        while (endPosition <= TERM_LENGTH);

        assertCanReadValueAt(TERM_LENGTH + DataHeaderFlyweight.HEADER_LENGTH);
    }

    private void assertCanReadValueAt(final int position)
    {
        archiveReader.read(STREAM_ID, publication.sessionId(), position,
            (messageFrame, srcBuffer, startOffset, messageOffset, messageLength) -> {
                assertEquals(VALUE, srcBuffer.getInt(startOffset + OFFSET));
                return false;
            });
    }

    private long writeBuffer()
    {
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
        quietClose(logger);
        quietClose(inboundStreams);
        quietClose(aeron);
        quietClose(mediaDriver);
    }

}
