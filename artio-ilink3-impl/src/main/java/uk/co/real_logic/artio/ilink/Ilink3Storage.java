/*
 * Copyright 2021 Monotonic Ltd.
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
package uk.co.real_logic.artio.ilink;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochNanoClock;
import uk.co.real_logic.artio.engine.framer.ILink3Context;
import uk.co.real_logic.artio.engine.framer.ILink3Key;
import uk.co.real_logic.artio.fixp.AbstractFixPStorage;
import uk.co.real_logic.artio.fixp.FixPKey;
import uk.co.real_logic.artio.fixp.InternalFixPContext;
import uk.co.real_logic.artio.storage.messages.ILink3ContextDecoder;
import uk.co.real_logic.artio.storage.messages.ILink3ContextEncoder;

public class Ilink3Storage extends AbstractFixPStorage
{
    private final ILink3ContextEncoder contextEncoder = new ILink3ContextEncoder();
    private final ILink3ContextDecoder contextDecoder = new ILink3ContextDecoder();
    private final int actingBlockLength = contextEncoder.sbeBlockLength();
    private final int actingVersion = contextEncoder.sbeSchemaVersion();

    private final EpochNanoClock clock;

    public Ilink3Storage(final EpochNanoClock clock)
    {
        this.clock = clock;
    }

    public InternalFixPContext newInitiatorContext(final FixPKey key, final int offset)
    {
        final long newUuid = nanoSecondTimestamp();
        return new ILink3Context((ILink3Key)key, clock, 0, 0, newUuid, 0, true, offset);
    }

    public InternalFixPContext loadContext(
        final AtomicBuffer buffer, final int offset, final int fileVersion)
    {
        contextDecoder.wrap(buffer, offset, actingBlockLength, actingVersion);
        final long uuid = contextDecoder.uuid();
        final int port = contextDecoder.port();
        final String host = contextDecoder.host();
        final String accessKeyId = contextDecoder.accessKeyId();

        final ILink3Key key = new ILink3Key(port, host, accessKeyId);
        return new ILink3Context(key, clock, uuid, 0, uuid, 0, false, offset);
    }

    public int saveContext(
        final InternalFixPContext fixPContext, final AtomicBuffer buffer, final int offset, final int fileVersion)
    {
        final ILink3Context context = (ILink3Context)fixPContext;
        final ILink3Key key = context.key();
        contextEncoder
            .wrap(buffer, offset)
            .uuid(context.uuid())
            .port(key.port())
            .host(key.host())
            .accessKeyId(key.accessKeyId());

        return contextEncoder.limit() - offset;
    }

    public void updateContext(final InternalFixPContext fixPContext, final AtomicBuffer buffer)
    {
        final ILink3Context context = (ILink3Context)fixPContext;
        contextEncoder
            .wrap(buffer, context.offset())
            .uuid(context.uuid());
    }

    private long nanoSecondTimestamp()
    {
        return clock.nanoTime();
    }
}
