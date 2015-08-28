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
package uk.co.real_logic.fix_gateway;

import org.HdrHistogram.Histogram;
import uk.co.real_logic.agrona.concurrent.NanoClock;

import static uk.co.real_logic.fix_gateway.CommonConfiguration.MESSAGES_EXCHANGED;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.WARMUP_MESSAGES;

public final class Timer
{
    public static final int TOTAL_MESSAGES = WARMUP_MESSAGES + MESSAGES_EXCHANGED;
    private final Histogram histogram;
    private final NanoClock clock;
    private final String name;

    private int count;

    public Timer(final String name, final NanoClock clock)
    {
        this.name = name;
        histogram = new Histogram(3);
        this.clock = clock;
    }

    public long recordSince(final long timestamp)
    {
        final long time = clock.nanoTime();
        final long duration = time - timestamp;
        histogram.recordValue(duration);
        count++;
        if (count == WARMUP_MESSAGES)
        {
            histogram.reset();
        }
        else if (count == TOTAL_MESSAGES)
        {
            prettyPrint(name, histogram);
        }
        return time;
    }

    public static void prettyPrint(final String name, final Histogram histogram)
    {
        prettyPrint(name, histogram, 1.0);
    }

    public static void prettyPrint(final String name, final Histogram histogram, final double scalingFactor)
    {
        System.out.printf("%s Histogram\n", name);
        System.out.println("----------");
        System.out.printf("Mean: %G\n", histogram.getMean() / scalingFactor);
        System.out.printf("1:    %G\n", histogram.getValueAtPercentile(1) / scalingFactor);
        System.out.printf("50:   %G\n", histogram.getValueAtPercentile(50) / scalingFactor);
        System.out.printf("90:   %G\n", histogram.getValueAtPercentile(90) / scalingFactor);
        System.out.printf("99:   %G\n", histogram.getValueAtPercentile(99) / scalingFactor);
        System.out.printf("99.9: %G\n", histogram.getValueAtPercentile(99.9) / scalingFactor);
        System.out.printf("100:  %G\n", histogram.getValueAtPercentile(100) / scalingFactor);
        System.out.println("----------");
        System.out.println();
    }

}
