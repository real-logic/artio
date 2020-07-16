/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.artio.util;

import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.fields.EpochFractionFormat;

import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link EpochFractionClock}
 * Based internally on a combination of {@link EpochClock} and {@link EpochNanoClock}
 */
public class CompositeEpochClock implements EpochFractionClock
{
    private final EpochClock clock;
    private final EpochNanoClock nanoClock;

    public CompositeEpochClock(final EpochClock clock, final EpochNanoClock nanoClock)
    {
        this.clock = clock;
        this.nanoClock = nanoClock;
    }

    public long time()
    {
        return clock.time();
    }

    public long epochFractionTime(final EpochFractionFormat format)
    {
        switch (format)
        {
            case NANOSECONDS:
                return nanoClock.nanoTime();

            case MICROSECONDS:
                return TimeUnit.NANOSECONDS.toMicros(nanoClock.nanoTime());

            case MILLISECONDS:
                return clock.time();

            default:
                throw new RuntimeException("Unknown precision: " + format);
        }
    }
}
