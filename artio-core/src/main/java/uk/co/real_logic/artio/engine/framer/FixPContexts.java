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

import org.agrona.ErrorHandler;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.engine.MappedFile;
import uk.co.real_logic.artio.engine.logger.LoggerUtil;
import uk.co.real_logic.artio.fixp.FirstMessageRejectReason;
import uk.co.real_logic.artio.fixp.FixPContext;
import uk.co.real_logic.artio.fixp.FixPKey;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;
import uk.co.real_logic.artio.storage.messages.ILink3ContextDecoder;
import uk.co.real_logic.artio.storage.messages.ILink3ContextEncoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_LONG;


class FixPContexts
{
    private final Map<ILink3Key, ILink3Context> keyToContext = new HashMap<>();
    private final MappedFile mappedFile;
    private final AtomicBuffer buffer;
    private final ErrorHandler errorHandler;
    private final EpochNanoClock epochNanoClock;
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final ILink3ContextEncoder contextEncoder = new ILink3ContextEncoder();
    private final ILink3ContextDecoder contextDecoder = new ILink3ContextDecoder();
    private final int actingBlockLength = contextEncoder.sbeBlockLength();
    private final int actingVersion = contextEncoder.sbeSchemaVersion();

    private final Long2LongHashMap authenticatedSessionIdToConnectionId = new Long2LongHashMap(MISSING_LONG);
    // TODO: persist this state, generify and replace keyToContext
    private final Map<FixPKey, FixPContext> acceptorKeyToContext = new HashMap<>();

    private int offset;

    FixPContexts(final MappedFile mappedFile, final ErrorHandler errorHandler, final EpochNanoClock epochNanoClock)
    {
        this.mappedFile = mappedFile;
        this.buffer = mappedFile.buffer();
        this.errorHandler = errorHandler;
        this.epochNanoClock = epochNanoClock;
        loadBuffer();
    }

    private void loadBuffer()
    {
        if (LoggerUtil.initialiseBuffer(
            buffer,
            headerEncoder,
            headerDecoder,
            contextEncoder.sbeSchemaId(),
            contextEncoder.sbeTemplateId(),
            actingVersion,
            actingBlockLength,
            errorHandler))
        {
            mappedFile.force();
        }

        offset = MessageHeaderEncoder.ENCODED_LENGTH;

        final int capacity = buffer.capacity();
        while (offset < capacity)
        {
            contextDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
            final long uuid = contextDecoder.uuid();
            if (uuid == 0)
            {
                break;
            }

            final int port = contextDecoder.port();
            final String host = contextDecoder.host();
            final String accessKeyId = contextDecoder.accessKeyId();

            keyToContext.put(
                new ILink3Key(port, host, accessKeyId),
                new ILink3Context(this, uuid, 0, uuid, 0, false, offset));

            offset = contextDecoder.limit();
        }
    }

    ILink3Context calculateUuid(
        final int port, final String host, final String accessKeyId, final boolean reestablishConnection)
    {
        final ILink3Key key = new ILink3Key(port, host, accessKeyId);

        final ILink3Context context = keyToContext.get(key);

        if (context != null)
        {
            final long connectLastUuid = context.uuid();
            context.connectLastUuid(connectLastUuid);

            // connectLastUuid == 0 implies that we're attempting to re-establish a connection that failed on its
            // last attempt, also its first of the week, so we need to generate a new UUID
            final boolean newlyAllocated = !reestablishConnection || connectLastUuid == 0;

            context.newlyAllocated(newlyAllocated);
            if (newlyAllocated)
            {
                final long newUuid = microSecondTimestamp();
                context.connectUuid(newUuid);
            }
            else
            {
                // We may have an invalid connect uuid from a failed connection at this point.
                context.connectUuid(connectLastUuid);
            }

            return context;
        }

        return allocateUuid(key);
    }

    private ILink3Context allocateUuid(final ILink3Key key)
    {
        final ILink3Context context = newUuid();
        keyToContext.put(key, context);
        return context;
    }

    private ILink3Context newUuid()
    {
        final long newUuid = microSecondTimestamp();
        return new ILink3Context(this, 0, 0, newUuid, 0, true, offset);
    }

    void updateUuid(final ILink3Context context)
    {
        contextEncoder
            .wrap(buffer, context.offset())
            .uuid(context.uuid());
    }

    void saveNewUuid(final ILink3Context context)
    {
        ILink3Key key = null;
        for (final Map.Entry<ILink3Key, ILink3Context> entry : keyToContext.entrySet())
        {
            if (entry.getValue() == context)
            {
                key = entry.getKey();
                break;
            }
        }

        if (null == key)
        {
            throw new IllegalStateException("expected to find key");
        }

        contextEncoder
            .wrap(buffer, offset)
            .uuid(context.uuid())
            .port(key.port)
            .host(key.host)
            .accessKeyId(key.accessKeyId);

        offset = contextEncoder.limit();
    }

    private long microSecondTimestamp()
    {
        return epochNanoClock.nanoTime();
    }

    int offset()
    {
        return offset;
    }

    public void close()
    {
        mappedFile.close();
    }

    public FirstMessageRejectReason onAcceptorLogon(
        final long sessionId, final FixPContext context, final long connectionId)
    {
        final long duplicateConnection = authenticatedSessionIdToConnectionId.get(sessionId);
        if (duplicateConnection == MISSING_LONG)
        {
            authenticatedSessionIdToConnectionId.put(sessionId, connectionId);

            final FixPKey key = context.toKey();
            final FixPContext oldContext = acceptorKeyToContext.get(key);
            final FirstMessageRejectReason rejectReason = context.checkConnect(oldContext);
            if (rejectReason == null)
            {
                acceptorKeyToContext.put(key, context);
            }
            return rejectReason;
        }
        else
        {
            return FirstMessageRejectReason.NEGOTIATE_DUPLICATE_ID;
        }
    }

    public void onDisconnect(final long connectionId)
    {
        final Long2LongHashMap.EntryIterator it = authenticatedSessionIdToConnectionId.entrySet().iterator();
        while (it.hasNext())
        {
            it.next();

            if (it.getLongValue() == connectionId)
            {
                it.remove();
            }
        }
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

}
