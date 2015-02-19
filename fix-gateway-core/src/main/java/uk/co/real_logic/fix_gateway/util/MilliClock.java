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
package uk.co.real_logic.fix_gateway.util;

/**
 * Functional interface for return the current time as system wide monotonic tick of 1 millisecond precision.
 */
@FunctionalInterface
public interface MilliClock
{
    /**
     * The number of ticks in millisecond the clock has advanced since starting.
     *
     * @return number of ticks in millisecond the clock has advanced since starting.
     */
    long time();
}
