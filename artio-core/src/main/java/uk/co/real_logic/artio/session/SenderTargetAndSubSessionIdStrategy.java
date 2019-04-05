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
package uk.co.real_logic.artio.session;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import uk.co.real_logic.artio.builder.HeaderEncoder;
import uk.co.real_logic.artio.decoder.HeaderDecoder;
import uk.co.real_logic.artio.dictionary.generation.CodecUtil;
import uk.co.real_logic.artio.storage.messages.SenderTargetAndSubCompositeKeyDecoder;
import uk.co.real_logic.artio.storage.messages.SenderTargetAndSubCompositeKeyEncoder;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * A simple, and dumb session id Strategy based upon hashing SenderCompID and TargetCompID. Makes no assumptions
 * about the nature of either identifiers.
 */
class SenderTargetAndSubSessionIdStrategy implements SessionIdStrategy
{
    private static final int BLOCK_AND_LENGTH_FIELDS_LENGTH = SenderTargetAndSubCompositeKeyEncoder.BLOCK_LENGTH + 6;

    private final SenderTargetAndSubCompositeKeyEncoder keyEncoder = new SenderTargetAndSubCompositeKeyEncoder();
    private final SenderTargetAndSubCompositeKeyDecoder keyDecoder = new SenderTargetAndSubCompositeKeyDecoder();
    private final int actingBlockLength = keyDecoder.sbeBlockLength();
    private final int actingVersion = keyDecoder.sbeSchemaVersion();

    SenderTargetAndSubSessionIdStrategy()
    {
    }

    public CompositeKey onAcceptLogon(final HeaderDecoder header)
    {
        return new CompositeKeyImpl(
            header.targetCompID(), header.targetCompIDLength(),
            header.senderSubID(), header.senderSubIDLength(),
            header.senderCompID(), header.senderCompIDLength());
    }

    public CompositeKey onInitiateLogon(
        final String localCompId,
        final String localSubId,
        final String localLocationId,
        final String remoteCompId,
        final String remoteSubId,
        final String remoteLocationId)
    {
        final char[] senderCompIdChars = localCompId.toCharArray();
        final char[] senderSubIdChars = localSubId.toCharArray();
        final char[] targetCompIdChars = remoteCompId.toCharArray();
        return new CompositeKeyImpl(
            senderCompIdChars,
            senderCompIdChars.length,
            senderSubIdChars,
            senderSubIdChars.length,
            targetCompIdChars,
            targetCompIdChars.length);
    }

    public void setupSession(final CompositeKey compositeKey, final HeaderEncoder headerEncoder)
    {
        final CompositeKeyImpl composite = (CompositeKeyImpl)compositeKey;
        headerEncoder.senderCompID(composite.localCompId);
        headerEncoder.senderSubID(composite.localSubID);
        headerEncoder.targetCompID(composite.remoteCompID);
    }

    public int save(final CompositeKey compositeKey, final MutableDirectBuffer buffer, final int offset)
    {
        final CompositeKeyImpl key = (CompositeKeyImpl)compositeKey;
        final byte[] localCompID = key.localCompId;
        final byte[] localSubID = key.localSubID;
        final byte[] remoteCompID = key.remoteCompID;

        final int length =
            localCompID.length + localSubID.length + remoteCompID.length + BLOCK_AND_LENGTH_FIELDS_LENGTH;

        if (buffer.capacity() < offset + length)
        {
            return INSUFFICIENT_SPACE;
        }

        keyEncoder.wrap(buffer, offset);
        keyEncoder.putLocalCompId(localCompID, 0, localCompID.length);
        keyEncoder.putLocalSubId(localSubID, 0, localSubID.length);
        keyEncoder.putRemoteCompId(remoteCompID, 0, remoteCompID.length);

        return length;
    }

    public CompositeKey load(final DirectBuffer buffer, final int offset, final int length)
    {
        keyDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);

        final int localCompIdLength = keyDecoder.localCompIdLength();
        final byte[] localCompId = new byte[localCompIdLength];
        keyDecoder.getLocalCompId(localCompId, 0, localCompIdLength);

        final int localSubIdLength = keyDecoder.localSubIdLength();
        final byte[] localSubId = new byte[localSubIdLength];
        keyDecoder.getLocalSubId(localSubId, 0, localSubIdLength);

        final int remoteCompIdLength = keyDecoder.remoteCompIdLength();
        final byte[] remoteCompId = new byte[remoteCompIdLength];
        keyDecoder.getRemoteCompId(remoteCompId, 0, remoteCompIdLength);

        return new CompositeKeyImpl(localCompId, localSubId, remoteCompId);
    }

    private static final class CompositeKeyImpl implements CompositeKey
    {
        private final byte[] localCompId;
        private final byte[] localSubID;
        private final byte[] remoteCompID;
        private final int hashCode;

        private CompositeKeyImpl(
            final char[] localCompId,
            final int localCompIDLength,
            final char[] localSubID,
            final int localSubIDLength,
            final char[] remoteCompID,
            final int remoteCompIDLength)
        {
            this(
                CodecUtil.toBytes(localCompId, localCompIDLength),
                CodecUtil.toBytes(localSubID, localSubIDLength),
                CodecUtil.toBytes(remoteCompID, remoteCompIDLength));
        }

        private CompositeKeyImpl(
            final byte[] localCompId,
            final byte[] localSubID,
            final byte[] remoteCompID)
        {
            this.localCompId = localCompId;
            this.localSubID = localSubID;
            this.remoteCompID = remoteCompID;
            hashCode = hash(this.localCompId, this.localSubID, this.remoteCompID);
        }

        private int hash(final byte[] localCompID, final byte[] localSubID, final byte[] remoteCompID)
        {
            int result = Arrays.hashCode(localCompID);
            result = 31 * result + Arrays.hashCode(localSubID);
            result = 31 * result + Arrays.hashCode(remoteCompID);
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
                return Arrays.equals(compositeKey.localCompId, localCompId) &&
                    Arrays.equals(compositeKey.localSubID, localSubID) &&
                    Arrays.equals(compositeKey.remoteCompID, remoteCompID);
            }

            return false;
        }

        public String toString()
        {
            return "CompositeKey{" +
                "localCompId=" + localCompId() +
                "localSubId=" + localSubId() +
                ", remoteCompId=" + remoteCompId() +
                '}';
        }

        public String localCompId()
        {
            return new String(localCompId, US_ASCII);
        }

        public String localSubId()
        {
            return new String(localSubID, US_ASCII);
        }

        public String localLocationId()
        {
            return "";
        }

        public String remoteCompId()
        {
            return new String(remoteCompID, US_ASCII);
        }

        public String remoteSubId()
        {
            return "";
        }

        public String remoteLocationId()
        {
            return "";
        }
    }
}
