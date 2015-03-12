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
    private final Map<Entry, Long> identifiers = new HashMap<>();

    private long counter = 0;

    public long identify(final HeaderDecoder header)
    {
        return identify(header.senderCompID(), header.targetCompID());
    }

    public long identify(final char[] senderCompID, final char[] targetCompID)
    {
        final Entry entry = new Entry(senderCompID, targetCompID);
        Long identifier = identifiers.putIfAbsent(entry, counter);
        if (identifier == null)
        {
            identifier = counter;
        }

        if (identifier == counter)
        {
            counter++;
        }
        return identifier;
    }

    private static final class Entry
    {
        private final char[] senderCompID;
        private final char[] targetCompID;
        private final int hashCode;

        private Entry(final char[] senderCompID, final char[] targetCompID)
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
            if (obj instanceof Entry)
            {
                Entry entry = (Entry) obj;
                return Arrays.equals(entry.senderCompID, senderCompID)
                    && Arrays.equals(entry.targetCompID, targetCompID);
            }

            return false;
        }
    }
}
