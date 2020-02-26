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
package uk.co.real_logic.artio.engine.framer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static uk.co.real_logic.artio.library.ILink3Session.MICROS_IN_MILLIS;
import static uk.co.real_logic.artio.library.ILink3Session.NANOS_IN_MICROS;

public class ILink3Contexts
{
    private final Map<ILink3Key, ILink3Context> keyToContext = new HashMap<>();
    private final Function<ILink3Key, ILink3Context> newUuid = this::newUuid;

    public ILink3Contexts()
    {
        // TODO: load all contexts from file
    }

    public long calculateUuid(
        final int port, final String host, final String accessKeyId, final boolean reestablishConnection)
    {
        final ILink3Key key = new ILink3Key(port, host, accessKeyId);

        if (reestablishConnection)
        {
            return lookupUuid(key);
        }

        return allocateUuid(key);
    }

    private long allocateUuid(final ILink3Key key)
    {
        return newUuid(key).uuid;
    }

    private ILink3Context newUuid(final ILink3Key key)
    {
        final long newUuid = microSecondTimestamp();
        // TODO: save
        final ILink3Context context = new ILink3Context(newUuid, 0);
        keyToContext.put(key, context);
        return context;
    }

    private long lookupUuid(final ILink3Key key)
    {
        return keyToContext.computeIfAbsent(key, newUuid).uuid;
    }

    private long microSecondTimestamp()
    {
        final long microseconds = (NANOS_IN_MICROS * System.nanoTime()) % MICROS_IN_MILLIS;
        return MILLISECONDS.toMicros(System.currentTimeMillis()) + microseconds;
    }

    private static final class ILink3Key
    {
        private final int port;
        private final String host;
        private final String accessKeyId;

        private ILink3Key(final int port, final String host, final String accessKeyId)
        {
            this.port = port;
            this.host = host;
            this.accessKeyId = accessKeyId;
        }

        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final ILink3Key iLink3Key = (ILink3Key)o;

            if (port != iLink3Key.port)
            {
                return false;
            }
            if (!Objects.equals(host, iLink3Key.host))
            {
                return false;
            }
            return Objects.equals(accessKeyId, iLink3Key.accessKeyId);
        }

        public int hashCode()
        {
            int result = port;
            result = 31 * result + (host != null ? host.hashCode() : 0);
            result = 31 * result + (accessKeyId != null ? accessKeyId.hashCode() : 0);
            return result;
        }
    }

    private static final class ILink3Context
    {
        private final long uuid;
        private final long position;

        private ILink3Context(final long uuid, final long position)
        {
            this.uuid = uuid;
            this.position = position;
        }
    }
}
