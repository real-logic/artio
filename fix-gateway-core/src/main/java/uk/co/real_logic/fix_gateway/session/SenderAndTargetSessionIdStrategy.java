/*
 * Copyright 2015-2016 Real Logic Ltd.
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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil;
import uk.co.real_logic.fix_gateway.messages.SenderAndTargetCompositeKeyDecoder;
import uk.co.real_logic.fix_gateway.messages.SenderAndTargetCompositeKeyEncoder;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

/**
 * A simple, and dumb session id Strategy based upon hashing SenderCompID and TargetCompID. Makes no assumptions
 * about the nature of either identifiers.
 *
 * Threadsafe - no state;
 */
public class SenderAndTargetSessionIdStrategy implements SessionIdStrategy
{
    private static final int BLOCK_AND_LENGTH_FIELDS_LENGTH = SenderAndTargetCompositeKeyEncoder.BLOCK_LENGTH + 4;

    private final SenderAndTargetCompositeKeyEncoder keyEncoder = new SenderAndTargetCompositeKeyEncoder();
    private final SenderAndTargetCompositeKeyDecoder keyDecoder = new SenderAndTargetCompositeKeyDecoder();
    private final int actingBlockLength = keyDecoder.sbeBlockLength();
    private final int actingVersion = keyDecoder.sbeSchemaVersion();

    public CompositeKey onLogon(final HeaderDecoder header)
    {
        requireNonNull(header, "header");

        return new CompositeKeyImpl(
            header.targetCompID(), header.targetCompIDLength(),
            header.senderCompID(), header.senderCompIDLength());
    }

    public CompositeKey onLogon(
        final String senderCompId, final String senderSubId, final String senderLocationId, final String targetCompId)
    {
        requireNonNull(senderCompId, "senderCompId");
        requireNonNull(targetCompId, "targetCompId");

        final char[] senderCompID = senderCompId.toCharArray();
        final char[] targetCompID = targetCompId.toCharArray();
        return new CompositeKeyImpl(
            senderCompID, senderCompID.length, targetCompID, targetCompID.length);
    }

    public void setupSession(final CompositeKey compositeKey, final HeaderEncoder headerEncoder)
    {
        requireNonNull(compositeKey, "compositeKey");
        requireNonNull(headerEncoder, "headerEncoder");

        final CompositeKeyImpl composite = (CompositeKeyImpl)compositeKey;
        headerEncoder.senderCompID(composite.senderCompID);
        headerEncoder.targetCompID(composite.targetCompID);
    }

    public int save(final CompositeKey compositeKey, final MutableDirectBuffer buffer, final int offset)
    {
        requireNonNull(compositeKey, "compositeKey");
        requireNonNull(buffer, "buffer");

        final CompositeKeyImpl key = (CompositeKeyImpl) compositeKey;
        final byte[] senderCompID = key.senderCompID;
        final byte[] targetCompID = key.targetCompID;

        final int length = senderCompID.length + targetCompID.length + BLOCK_AND_LENGTH_FIELDS_LENGTH;
        if (buffer.capacity() < offset + length)
        {
            return INSUFFICIENT_SPACE;
        }

        keyEncoder.wrap(buffer, offset);
        keyEncoder.putSenderCompId(senderCompID, 0, senderCompID.length);
        keyEncoder.putTargetCompId(targetCompID, 0, targetCompID.length);

        return length;
    }

    public CompositeKey load(final DirectBuffer buffer, final int offset, final int length)
    {
        requireNonNull(buffer, "buffer");

        keyDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);

        final int senderCompIdLength = keyDecoder.senderCompIdLength();
        final byte[] senderCompId = new byte[senderCompIdLength];
        keyDecoder.getSenderCompId(senderCompId, 0, senderCompIdLength);

        final int targetCompIdLength = keyDecoder.targetCompIdLength();
        final byte[] targetCompId = new byte[targetCompIdLength];
        keyDecoder.getTargetCompId(targetCompId, 0, targetCompIdLength);

        return new CompositeKeyImpl(senderCompId, targetCompId);
    }

    private static final class CompositeKeyImpl implements CompositeKey
    {
        private final byte[] senderCompID;
        private final byte[] targetCompID;
        private final int hashCode;

        private CompositeKeyImpl(final char[] senderCompID,
                                 final int senderCompIDLength,
                                 final char[] targetCompID,
                                 final int targetCompIDLength)
        {
            this(
                CodecUtil.toBytes(senderCompID, senderCompIDLength),
                CodecUtil.toBytes(targetCompID, targetCompIDLength));
        }

        private CompositeKeyImpl(final byte[] senderCompID, final byte[] targetCompID)
        {
            this.senderCompID = senderCompID;
            this.targetCompID = targetCompID;
            hashCode = hash(senderCompID, targetCompID);
        }

        private int hash(final byte[] senderCompID, final byte[] targetCompID)
        {
            int result  = Arrays.hashCode(senderCompID);
            result = 31 * result + Arrays.hashCode(targetCompID);
            return result;
        }

        public int hashCode()
        {
            return hashCode;
        }

        public boolean equals(final Object obj)
        {
            if (obj instanceof CompositeKeyImpl)
            {
                final CompositeKeyImpl compositeKey = (CompositeKeyImpl)obj;
                return Arrays.equals(compositeKey.senderCompID, senderCompID)
                    && Arrays.equals(compositeKey.targetCompID, targetCompID);
            }

            return false;
        }

        public String toString()
        {
            return "CompositeKey{" +
                "senderCompID=" + Arrays.toString(senderCompID) +
                ", targetCompID=" + Arrays.toString(targetCompID) +
                '}';
        }

        public String senderCompId()
        {
            return new String(senderCompID, US_ASCII);
        }

        public String senderSubId()
        {
            return "";
        }

        public String senderLocationId()
        {
            return "";
        }

        public String targetCompId()
        {
            return new String(targetCompID, US_ASCII);
        }
    }
}
