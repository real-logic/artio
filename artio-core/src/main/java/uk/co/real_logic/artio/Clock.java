/*
 * Copyright 2014-2018 Real Logic Limited.
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
package uk.co.real_logic.artio;

/**
 * The clock interface supports different sources of time to be plugged into Artio.
 *
 * This enables you to have different timestamp precisions, depending upon what your use
 * case is. You can use nanosecond precision if you're most interested in timing performance
 * of the messages going through the system. You could also choose to use milliseconds if
 * You would prefer a stable measure time of time.
 */
@FunctionalInterface
public interface Clock
{
    static Clock systemNanoTime()
    {
        return System::nanoTime;
    }

    /**
     * Get the current time.
     *
     * @return the current time.
     */
    long time();
}
