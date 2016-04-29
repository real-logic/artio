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
package uk.co.real_logic.fix_gateway.timing;

import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class HistogramLoggingTest
{

    private HistogramLogHandler logHandler = mock(HistogramLogHandler.class);
    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private File file;
    private Timer timer;
    private HistogramLogWriter writer;
    private HistogramLogReader reader;

    @Before
    public void setUp() throws Exception
    {
        file = File.createTempFile("histogram", "tmp");
        timer = new Timer("abc", 1);
        writer = new HistogramLogWriter(
            Arrays.asList(timer),
            file.getAbsolutePath(),
            100,
            errorHandler);
        reader = new HistogramLogReader(file);
    }

    @Test
    public void shouldWriteAndReadAHistogram() throws Exception
    {
        timer.recordValue(20);
        timer.recordValue(50);
        timer.recordValue(33);
        timer.recordValue(52);
        timer.recordValue(200);
        timer.recordValue(5);

        assertThat(writer.doWork(), greaterThan(0));

        assertEquals(1, reader.read(logHandler));

        assertEquals("Tried to read more data again after first read", 0, reader.read(logHandler));
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
}
