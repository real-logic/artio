/*
 * Copyright 2015-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.timing;

import org.HdrHistogram.Histogram;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.CommonConfiguration.DEFAULT_NAME_PREFIX;

public class HistogramLoggingTest
{
    private static final String NAME = "abc";

    private static final HistogramHandler NO_HISTOGRAM_HANDLER = null;

    private final EpochClock clock = mock(EpochClock.class);
    private final HistogramLogHandler logHandler = mock(HistogramLogHandler.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final ArgumentCaptor<Histogram> histogramCaptor = ArgumentCaptor.forClass(Histogram.class);

    private File file;
    private Timer timer;
    private HistogramLogAgent writer;
    private HistogramLogReader reader;

    @Before
    public void setUp() throws Exception
    {
        when(clock.time()).thenReturn(110L, 220L, 330L, 440L);

        file = Files.createTempFile("histogram", "tmp").toFile();
        timer = new Timer(clock::time, NAME, 1, mock(AtomicCounter.class));
        writer = new HistogramLogAgent(
            Collections.singletonList(timer),
            file.getAbsolutePath(),
            100,
            errorHandler,
            clock,
            NO_HISTOGRAM_HANDLER,
            DEFAULT_NAME_PREFIX);
        reader = new HistogramLogReader(file);
    }

    @After
    public void tearDown()
    {
        try
        {
            writer.onClose();
            CloseHelper.close(reader);
        }
        finally
        {
            file.delete();
        }
    }

    @Test
    public void shouldWriteAndReadAHistogram() throws Exception
    {
        recordValues();

        writeHistogram();

        readsHistogram(6);

        readsNothing();
    }

    @Test
    public void shouldWriteAndReadMultipleHistograms() throws Exception
    {
        shouldWriteAndReadAHistogram();

        writeHistogram();

        readsHistogram(0);

        recordValues();

        writeHistogram();

        readsHistogram(6);
    }

    private void writeHistogram()
    {
        assertThat(writer.doWork(), greaterThan(0));
    }

    private void readsNothing() throws IOException
    {
        assertEquals("Tried to read more data again after first read", 0, reader.read(logHandler));
    }

    private void readsHistogram(final int expectedCount) throws IOException
    {
        reset(logHandler);
        assertEquals(1, reader.read(logHandler));
        verify(logHandler, times(1)).onHistogram(anyLong(), eq(NAME), histogramCaptor.capture());
        assertEquals("Histogram returns unexpected count", expectedCount, histogram().getTotalCount());
    }

    private void recordValues()
    {
        timer.recordValue(20);
        timer.recordValue(50);
        timer.recordValue(33);
        timer.recordValue(52);
        timer.recordValue(200);
        timer.recordValue(5);
    }

    private Histogram histogram()
    {
        return histogramCaptor.getValue();
    }
}
