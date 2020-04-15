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
package uk.co.real_logic.artio.util;

import java.time.Instant;
import java.time.temporal.ChronoField;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class TimeUtil
{
    private static final long MICROS_IN_MILLIS = 1_000;
    private static final long NANOS_IN_MICROS = 1_000;

    public static long microSecondTimestamp()
    {
        final long microseconds = (System.nanoTime() / NANOS_IN_MICROS) % MICROS_IN_MILLIS;
        return MILLISECONDS.toMicros(System.currentTimeMillis()) + microseconds;
    }

    public static long nanoSecondTimestamp()
    {
        /*final long nanoseconds = System.nanoTime() % (NANOS_IN_MICROS * MICROS_IN_MILLIS);
        System.out.println("nanoseconds = " + nanoseconds);
        final Instant now = Instant.now();
        System.out.println(now.getNano());
        System.out.println(now.getLong(ChronoField.));*/
        return MILLISECONDS.toNanos(System.currentTimeMillis());
    }
}
