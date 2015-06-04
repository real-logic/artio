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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SessionIds
{
    private static final AtomicLong COUNTER = new AtomicLong(0);

    private final Map<Object, Long> compositeToSurrogate = new HashMap<>();

    public long onLogon(final Object compositeKey)
    {
        return compositeToSurrogate.computeIfAbsent(compositeKey, key -> COUNTER.getAndIncrement());
    }

    public void onDisconnect(final Object compositeKey)
    {
        compositeToSurrogate.remove(compositeKey);
    }

}
