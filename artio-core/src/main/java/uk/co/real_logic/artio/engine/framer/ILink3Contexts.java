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
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.engine.MappedFile;
import uk.co.real_logic.artio.engine.logger.LoggerUtil;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;
import uk.co.real_logic.artio.storage.messages.ILink3ContextDecoder;
import uk.co.real_logic.artio.storage.messages.ILink3ContextEncoder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


class ILink3Contexts
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

    private int offset;

    ILink3Contexts(final MappedFile mappedFile, final ErrorHandler errorHandler, final EpochNanoClock epochNanoClock)
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
                new ILink3Context(uuid, 0, false));

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
            context.lastUuid(context.uuid());
            context.newlyAllocated(!reestablishConnection);

            if (!reestablishConnection)
            {
                final long newUuid = microSecondTimestamp();
                context.uuid(newUuid);
            }

            return context;
        }

        return allocateUuid(key);
    }

    private ILink3Context allocateUuid(final ILink3Key key)
    {
        final ILink3Context context = newUuid(key);
        keyToContext.put(key, context);
        return context;
    }

    private ILink3Context newUuid(final ILink3Key key)
    {
        final long newUuid = microSecondTimestamp();
        contextEncoder
            .wrap(buffer, offset)
            .uuid(newUuid)
            .port(key.port)
            .host(key.host)
            .accessKeyId(key.accessKeyId);

        final ILink3Context context = new ILink3Context(newUuid, 0, true);
        offset = contextEncoder.limit();
        return context;
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
