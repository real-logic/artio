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
package uk.co.real_logic.fix_gateway.replication;

import java.util.concurrent.ThreadLocalRandom;

public class RandomTimeout
{
    private static final int MIN_TIMEOUT_FACTOR = 2;

    private final long maxTimeout;
    private final long minTimeout;

    private long nextExpiry;

    public RandomTimeout(final long maxTimeout, final long timeInMs)
    {
        this.maxTimeout = maxTimeout;
        this.minTimeout = maxTimeout / MIN_TIMEOUT_FACTOR;
        onKeepAlive(timeInMs);
    }

    public void onKeepAlive(final long timeInMs)
    {
        nextExpiry = timeInMs + ThreadLocalRandom.current().nextLong(minTimeout, maxTimeout);
    }

    public boolean hasTimedOut(final long timeInMs)
    {
        return timeInMs > nextExpiry;
    }

}
