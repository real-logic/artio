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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil;
import uk.co.real_logic.fix_gateway.storage.messages.SenderAndTargetCompositeKeyDecoder;
import uk.co.real_logic.fix_gateway.storage.messages.SenderAndTargetCompositeKeyEncoder;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

/**
 * A simple, and dumb session id Strategy based upon hashing SenderCompID and TargetCompID. Makes no assumptions
 * about the nature of either identifiers.
 */
class SenderAndTargetSessionIdStrategy implements SessionIdStrategy
{
    private static final int BLOCK_AND_LENGTH_FIELDS_LENGTH = SenderAndTargetCompositeKeyEncoder.BLOCK_LENGTH + 4;

    private final SenderAndTargetCompositeKeyEncoder keyEncoder = new SenderAndTargetCompositeKeyEncoder();
    private final SenderAndTargetCompositeKeyDecoder keyDecoder = new SenderAndTargetCompositeKeyDecoder();
    private final int actingBlockLength = keyDecoder.sbeBlockLength();
    private final int actingVersion = keyDecoder.sbeSchemaVersion();

    SenderAndTargetSessionIdStrategy()
    {
    }

    public CompositeKey onLogon(final HeaderDecoder header)
    {
        requireNonNull(header, "header");

        return new CompositeKeyImpl(
            header.targetCompID(), header.targetCompIDLength(),
            header.senderCompID(), header.senderCompIDLength());
    }

    public CompositeKey onLogon(
        final String senderCompId,
        final String senderSubId,
        final String senderLocationId,
        final String targetCompId)
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
        headerEncoder.senderCompID(composite.localCompID);
        headerEncoder.targetCompID(composite.remoteCompID);
    }

    public int save(final CompositeKey compositeKey, final MutableDirectBuffer buffer, final int offset)
    {
        requireNonNull(compositeKey, "compositeKey");
        requireNonNull(buffer, "buffer");

        final CompositeKeyImpl key = (CompositeKeyImpl) compositeKey;
        final byte[] senderCompID = key.localCompID;
        final byte[] targetCompID = key.remoteCompID;

        final int length = senderCompID.length + targetCompID.length + BLOCK_AND_LENGTH_FIELDS_LENGTH;
        if (buffer.capacity() < offset + length)
        {
            return INSUFFICIENT_SPACE;
        }

        keyEncoder.wrap(buffer, offset);
        keyEncoder.putLocalCompId(senderCompID, 0, senderCompID.length);
        keyEncoder.putRemoteCompId(targetCompID, 0, targetCompID.length);

        return length;
    }

    public CompositeKey load(final DirectBuffer buffer, final int offset, final int length)
    {
        requireNonNull(buffer, "buffer");

        keyDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);

        final int localCompIdLength = keyDecoder.localCompIdLength();
        final byte[] localCompId = new byte[localCompIdLength];
        keyDecoder.getLocalCompId(localCompId, 0, localCompIdLength);

        final int remoteCompIdLength = keyDecoder.remoteCompIdLength();
        final byte[] remoteCompId = new byte[remoteCompIdLength];
        keyDecoder.getRemoteCompId(remoteCompId, 0, remoteCompIdLength);

        return new CompositeKeyImpl(localCompId, remoteCompId);
    }

    private static final class CompositeKeyImpl implements CompositeKey
    {
        private final byte[] localCompID;
        private final byte[] remoteCompID;
        private final int hashCode;

        private CompositeKeyImpl(final char[] localCompID,
                                 final int localCompIDLength,
                                 final char[] remoteCompID,
                                 final int remoteCompIDLength)
        {
            this(
                CodecUtil.toBytes(localCompID, localCompIDLength),
                CodecUtil.toBytes(remoteCompID, remoteCompIDLength));
        }

        private CompositeKeyImpl(final byte[] localCompID, final byte[] remoteCompID)
        {
            this.localCompID = localCompID;
            this.remoteCompID = remoteCompID;
            hashCode = hash(localCompID, remoteCompID);
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
                return Arrays.equals(compositeKey.localCompID, localCompID)
                    && Arrays.equals(compositeKey.remoteCompID, remoteCompID);
            }

            return false;
        }

        public String toString()
        {
            return "CompositeKey{" +
                "localCompId=" + localCompId() +
                ", remoteCompId=" + remoteCompId() +
                '}';
        }

        public String localCompId()
        {
            return new String(localCompID, US_ASCII);
        }

        public String localSubId()
        {
            return "";
        }

        public String localLocationId()
        {
            return "";
        }

        public String remoteCompId()
        {
            return new String(remoteCompID, US_ASCII);
        }
    }
}
