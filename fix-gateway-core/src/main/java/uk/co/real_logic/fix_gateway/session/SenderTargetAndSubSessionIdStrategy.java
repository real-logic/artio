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

import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil;

import java.util.Arrays;

/**
 * A simple, and dumb session id Strategy based upon hashing SenderCompID and TargetCompID. Makes no assumptions
 * about the nature of either identifiers.
 *
 * Threadsafe - no state;
 */
public class SenderTargetAndSubSessionIdStrategy implements SessionIdStrategy
{
    public Object onAcceptorLogon(final HeaderDecoder header)
    {
        return new CompositeKey(
            header.targetCompID(), header.targetCompIDLength(),
            header.targetSubID(), header.targetSubIDLength(),
            header.senderCompID(), header.senderCompIDLength());
    }

    public Object onInitiatorLogon(
        final String senderCompId, final String senderSubId, final String senderLocationId, final String targetCompId)
    {
        final char[] senderCompIdChars = senderCompId.toCharArray();
        final char[] senderSubIdChars = senderSubId.toCharArray();
        final char[] targetCompIdChars = targetCompId.toCharArray();
        return new CompositeKey(
            senderCompIdChars,
            senderCompIdChars.length,
            senderSubIdChars,
            senderSubIdChars.length,
            targetCompIdChars,
            targetCompIdChars.length);
    }

    public void setupSession(final Object compositeKey, final HeaderEncoder headerEncoder)
    {
        final CompositeKey composite = (CompositeKey) compositeKey;
        headerEncoder.senderCompID(composite.senderCompID);
        headerEncoder.senderSubID(composite.senderSubID);
        headerEncoder.targetCompID(composite.targetCompID);
    }

    public int save(final Object compositeKey, final AtomicBuffer buffer, final int offset)
    {
        return INSUFFICIENT_SPACE;
    }

    public Object load(final AtomicBuffer buffer, final int offset, final int length)
    {
        return null;
    }

    private static final class CompositeKey
    {
        private final byte[] senderCompID;
        private final byte[] senderSubID;
        private final byte[] targetCompID;
        private final int hashCode;

        private CompositeKey(
            final char[] senderCompID,
            final int senderCompIDLength,
            final char[] senderSubID,
            final int senderSubIDLength,
            final char[] targetCompID,
            final int targetCompIDLength)
        {
            this.senderCompID = CodecUtil.toBytes(senderCompID, senderCompIDLength);
            this.senderSubID = CodecUtil.toBytes(senderSubID, senderSubIDLength);
            this.targetCompID = CodecUtil.toBytes(targetCompID, targetCompIDLength);

            hashCode = hash(this.senderCompID, this.senderSubID, this.targetCompID);
        }

        private int hash(final byte[] senderCompID, final byte[] senderSubID, final byte[] targetCompID)
        {
            int result  = Arrays.hashCode(senderCompID);
            result = 31 * result + Arrays.hashCode(senderSubID);
            result = 31 * result + Arrays.hashCode(targetCompID);
            return result;
        }

        public int hashCode()
        {
            return hashCode;
        }

        public boolean equals(final Object obj)
        {
            if (obj instanceof CompositeKey)
            {
                final CompositeKey compositeKey = (CompositeKey)obj;
                return Arrays.equals(compositeKey.senderCompID, senderCompID)
                    && Arrays.equals(compositeKey.senderSubID, senderSubID)
                    && Arrays.equals(compositeKey.targetCompID, targetCompID);
            }

            return false;
        }

        public String toString()
        {
            return "CompositeKey{" +
                "senderCompID=" + Arrays.toString(senderCompID) +
                "senderSubID=" + Arrays.toString(senderSubID) +
                ", targetCompID=" + Arrays.toString(targetCompID) +
                '}';
        }
    }
}
