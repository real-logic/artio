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
import org.agrona.IoUtil;
import org.agrona.collections.Int2ObjectHashMap;

import java.io.File;
import java.nio.MappedByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class HistogramLogReader implements AutoCloseable
{
    public static void main(String[] args)
    {
        if (args.length != 1)
        {
            System.err.println("Usage: HistogramLogReader <logFile>");
            System.err.println("Where <logFile> is the path to histogram log file");
            System.exit(-1);
        }

        final String file = args[0];
        final double scalingFactor = MICROSECONDS.toNanos(1);

        try (final HistogramLogReader logReader = new HistogramLogReader(file))
        {
            logReader.read((recordedAtTime, name, histogram) ->
                prettyPrint(recordedAtTime, histogram, name, scalingFactor));
        }
    }

    private final Int2ObjectHashMap<String> idToName = new Int2ObjectHashMap<>();
    private final MappedByteBuffer buffer;

    public HistogramLogReader(final String file)
    {
        buffer = map(file);
        readHeader();
    }

    private MappedByteBuffer map(final String path)
    {
        final File file = new File(path);
        return IoUtil.mapExistingFile(file, file.getName());
    }

    private void readHeader()
    {
        final int timerCount = buffer.getInt();
        for (int i = 0; i < timerCount; i++)
        {
            final int id = buffer.getInt();
            final byte[] nameBytes = new byte[buffer.getInt()];
            buffer.get(nameBytes);
            final String name = new String(nameBytes, UTF_8);
            idToName.put(id, name);
        }
    }

    private void read(final HistogramLogHandler handler)
    {
        final int timerCount = idToName.size();
        while (true)
        {
            final long timeStamp = buffer.getLong();
            if (timeStamp == 0)
            {
                return;
            }

            for (int i = 0; i < timerCount; i++)
            {
                final int id = buffer.getInt();
                final String name = idToName.get(id);
                final Histogram histogram = Histogram.decodeFromByteBuffer(buffer, 0);
                handler.onHistogram(timeStamp, name, histogram);
            }
        }
    }

    public void close()
    {
        IoUtil.unmap(buffer);
    }

    public static void prettyPrint(
        final long timestampInMs,
        final Histogram histogram,
        final String name,
        final double scalingFactor)
    {
        System.out.printf(
            "%s Histogram @ %dmillis\n" +
            "----------\n" +
            "Mean: %G\n" +
            "1:    %G\n" +
            "50:   %G\n" +
            "90:   %G\n" +
            "99:   %G\n" +
            "99.9: %G\n" +
            "100:  %G\n" +
            "----------\n",

            name,
            timestampInMs,
            histogram.getMean() / scalingFactor,
            scaledPercentile(histogram, scalingFactor, 1),
            scaledPercentile(histogram, scalingFactor, 50),
            scaledPercentile(histogram, scalingFactor, 90),
            scaledPercentile(histogram, scalingFactor, 99),
            scaledPercentile(histogram, scalingFactor, 99.9),
            scaledPercentile(histogram, scalingFactor, 100));
    }

    private static double scaledPercentile(final Histogram histogram,
                                           final double scalingFactor,
                                           final double percentile)
    {
        return histogram.getValueAtPercentile(percentile) / scalingFactor;
    }
}
