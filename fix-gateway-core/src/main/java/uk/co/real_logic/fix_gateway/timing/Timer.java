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

import org.HdrHistogram.Histogram;
import org.HdrHistogram.SingleWriterRecorder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static uk.co.real_logic.fix_gateway.CommonConfiguration.TIME_MESSAGES;

public class Timer
{
    public static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 3;

    // Only written to on recording thread
    private final SingleWriterRecorder recorder;

    private final String name;
    // Only accesssed upon logging thread
    private final int id;
    private Histogram histogram;

    public Timer(final String name, final int id)
    {
        this.name = name;
        this.id = id;
        recorder = new SingleWriterRecorder(NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);
    }

    public long recordSince(final long timestamp)
    {
        if (TIME_MESSAGES)
        {
            final long time = System.nanoTime();
            final long duration = time - timestamp;
            recordValue(duration);
            return time;
        }

        return 0;
    }

    void recordValue(final long duration)
    {
        recorder.recordValue(duration);
    }

    public void writeName(final ByteBuffer buffer)
    {
        final byte[] name = this.name.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(id);
        buffer.putInt(name.length);
        buffer.put(name);
    }

    public void writeTimings(final ByteBuffer buffer)
    {
        buffer.putInt(id);
        histogram = recorder.getIntervalHistogram(histogram);
        histogram.encodeIntoByteBuffer(buffer);
    }

}
