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
package uk.co.real_logic.fix_gateway.framer.session;

import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple, and dumb session id Strategy based upon hashing SenderCompID and TargetCompID. Makes no assumptions
 * about the nature of either identifiers.
 *
 * Should be used sparingly.
 */
public class HashingSenderAndTargetSessionIdStrategy implements SessionIdStrategy
{
    private final Map<CompositeKey, Long> compositeToSurrogate = new HashMap<>();
    private final Long2ObjectHashMap<CompositeKey> surrogateToComposite = new Long2ObjectHashMap<>();

    private long counter = 0;

    public long decode(final HeaderDecoder header)
    {
        return decode(header.senderCompID(), header.targetCompID());
    }

    public void encode(final long sessionId, final HeaderEncoder encoder)
    {
        final CompositeKey compositeKey = surrogateToComposite.get(sessionId);
        encoder.senderCompID(compositeKey.senderCompID);
        encoder.targetCompID(compositeKey.targetCompID);
    }

    public long decode(final char[] senderCompID, final char[] targetCompID)
    {
        final CompositeKey compositeKey = new CompositeKey(senderCompID, targetCompID);
        Long identifier = compositeToSurrogate.putIfAbsent(compositeKey, counter);
        if (identifier == null)
        {
            identifier = counter;
            surrogateToComposite.put(identifier.longValue(), compositeKey);
        }

        if (identifier == counter)
        {
            counter++;
        }

        return identifier;
    }

    private static final class CompositeKey
    {
        private final char[] senderCompID;
        private final char[] targetCompID;
        private final int hashCode;

        private CompositeKey(final char[] senderCompID, final char[] targetCompID)
        {
            this.senderCompID = senderCompID;
            this.targetCompID = targetCompID;

            hashCode = hash(senderCompID, targetCompID);
        }

        private int hash(final char[] senderCompID, final char[] targetCompID)
        {
            final int senderHash = Arrays.hashCode(senderCompID) & 0xFF00;
            final int targetHash = Arrays.hashCode(targetCompID) & 0x00FF;
            return senderHash | targetHash;
        }

        public int hashCode()
        {
            return hashCode;
        }

        public boolean equals(final Object obj)
        {
            if (obj instanceof CompositeKey)
            {
                CompositeKey compositeKey = (CompositeKey) obj;
                return Arrays.equals(compositeKey.senderCompID, senderCompID)
                    && Arrays.equals(compositeKey.targetCompID, targetCompID);
            }

            return false;
        }
    }
}
