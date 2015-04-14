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
package uk.co.real_logic.fix_gateway.session;

import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class SessionIds
{
    private static AtomicLong counter = new AtomicLong(0);

    private final Map<Object, Long> compositeToSurrogate = new HashMap<>();
    private final Long2ObjectHashMap<Object> surrogateToComposite = new Long2ObjectHashMap<>();
    private final Queue<? super NewSessionId> commandQueue;

    public SessionIds(final Queue<? super NewSessionId> commandQueue)
    {
        this.commandQueue = commandQueue;
    }

    public long onLogon(final Object compositeKey)
    {
        return compositeToSurrogate.computeIfAbsent(compositeKey, key ->
        {
            final long newSurrogateKey = counter.getAndIncrement();
            final NewSessionId newSessionId = new NewSessionId(key, newSurrogateKey);
            surrogateToComposite.put(newSurrogateKey, key);
            while (!commandQueue.offer(newSessionId))
            {
                // TODO: backoff
            }
            return newSurrogateKey;
        });
    }

    public void put(final Object compositeId, final long surrogateId)
    {
        compositeToSurrogate.put(compositeId, surrogateId);
        surrogateToComposite.put(surrogateId, compositeId);
    }

}
