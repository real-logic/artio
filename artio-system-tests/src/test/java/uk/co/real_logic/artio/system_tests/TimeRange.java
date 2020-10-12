/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.system_tests;

import org.agrona.concurrent.EpochNanoClock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TimeRange
{
    private final EpochNanoClock clock;
    private final long startTime;
    private long endTime;

    public TimeRange(final EpochNanoClock clock)
    {
        this.clock = clock;
        startTime = clock.nanoTime();
    }

    public void end()
    {
        endTime = clock.nanoTime();
    }

    public void assertWithinRange(final long time)
    {
        assertThat(time, greaterThanOrEqualTo(startTime));
        assertThat(time, lessThanOrEqualTo(endTime));
    }
}
