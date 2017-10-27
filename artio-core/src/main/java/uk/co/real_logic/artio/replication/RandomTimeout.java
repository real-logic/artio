/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.replication;

import java.util.concurrent.ThreadLocalRandom;

class RandomTimeout
{
    static final int MAX_TO_MIN_TIMEOUT = 2;

    private final long maxTimeout;
    private final long minTimeout;

    private long nextExpiry;

    RandomTimeout(final long timeout, final long timeInMs)
    {
        this.minTimeout = timeout;
        this.maxTimeout = timeout * MAX_TO_MIN_TIMEOUT;
        onKeepAlive(timeInMs);
    }

    void onKeepAlive(final long timeInMs)
    {
        final long timeout = ThreadLocalRandom.current().nextLong(minTimeout, maxTimeout);
        nextExpiry = timeInMs + timeout;
    }

    boolean hasTimedOut(final long timeInMs)
    {
        return timeInMs > nextExpiry;
    }

}
