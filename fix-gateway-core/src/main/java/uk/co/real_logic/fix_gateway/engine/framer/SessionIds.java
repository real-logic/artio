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
package uk.co.real_logic.fix_gateway.engine.framer;

import uk.co.real_logic.agrona.collections.LongHashSet;

import java.util.HashMap;
import java.util.Map;

public class SessionIds
{
    private static final long MISSING = -2;

    public static final long DUPLICATE_SESSION = -1;

    private static long counter = 0L;

    private final LongHashSet currentlyAuthenticated = new LongHashSet(40, MISSING);
    private final Map<Object, Long> compositeToSurrogate = new HashMap<>();

    public long onLogon(final Object compositeKey)
    {
        final Long sessionId = compositeToSurrogate.computeIfAbsent(compositeKey, key -> counter++);

        if (!currentlyAuthenticated.add(sessionId))
        {
            return DUPLICATE_SESSION;
        }

        return sessionId;
    }

    public void onDisconnect(final long compositeKey)
    {
        currentlyAuthenticated.remove(compositeKey);
    }

}
