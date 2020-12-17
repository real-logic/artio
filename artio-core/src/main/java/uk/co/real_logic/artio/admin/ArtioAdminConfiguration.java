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
package uk.co.real_logic.artio.admin;

import io.aeron.Aeron;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OffsetEpochNanoClock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static uk.co.real_logic.artio.CommonConfiguration.*;

public final class ArtioAdminConfiguration
{
    public static final int DEFAULT_INBOUND_ADMIN_STREAM = 21;
    public static final int DEFAULT_OUTBOUND_ADMIN_STREAM = 22;

    private final AtomicBoolean isConcluded = new AtomicBoolean(false);
    private final Aeron.Context aeronContext = new Aeron.Context();

    private IdleStrategy idleStrategy;
    private EpochNanoClock epochNanoClock;
    private int inboundAdminStream = DEFAULT_INBOUND_ADMIN_STREAM;
    private int outboundAdminStream = DEFAULT_OUTBOUND_ADMIN_STREAM;
    private String libraryAeronChannel = IPC_CHANNEL;
    private long replyTimeoutInNs = TimeUnit.MILLISECONDS.toNanos(DEFAULT_REPLY_TIMEOUT_IN_MS);

    public ArtioAdminConfiguration idleStrategy(final IdleStrategy idleStrategy)
    {
        this.idleStrategy = idleStrategy;
        return this;
    }

    public ArtioAdminConfiguration epochNanoClock(final EpochNanoClock epochNanoClock)
    {
        this.epochNanoClock = epochNanoClock;
        return this;
    }

    /**
     * Set the stream id from an admin API to a FIX Engine.
     *
     * @param inboundAdminStream the stream id from an admin API to a FIX Engine.
     * @return this
     */
    public ArtioAdminConfiguration inboundAdminStream(final int inboundAdminStream)
    {
        this.inboundAdminStream = inboundAdminStream;
        return this;
    }

    /**
     * Set the stream id from a FIX Engine to an admin API.
     *
     * @param outboundAdminStream the stream id from a FIX Engine to an admin API.
     * @return this
     */
    public ArtioAdminConfiguration outboundAdminStream(final int outboundAdminStream)
    {
        this.outboundAdminStream = outboundAdminStream;
        return this;
    }

    /**
     * Sets the aeron channel that libraries will use to communicate with the FixEngine instance.
     *
     * @param libraryAeronChannel the aeron channel that libraries will use to communicate with the FixEngine instance.
     * @return this
     */
    public ArtioAdminConfiguration libraryAeronChannel(final String libraryAeronChannel)
    {
        this.libraryAeronChannel = libraryAeronChannel;
        return this;
    }

    public ArtioAdminConfiguration replyTimeoutInNs(final long replyTimeoutInNs)
    {
        this.replyTimeoutInNs = replyTimeoutInNs;
        return this;
    }

    public Aeron.Context aeronContext()
    {
        return aeronContext;
    }

    public IdleStrategy idleStrategy()
    {
        return idleStrategy;
    }

    public EpochNanoClock epochNanoClock()
    {
        return epochNanoClock;
    }

    public int inboundAdminStream()
    {
        return inboundAdminStream;
    }

    public int outboundAdminStream()
    {
        return outboundAdminStream;
    }

    public String libraryAeronChannel()
    {
        return libraryAeronChannel;
    }

    public long replyTimeoutInNs()
    {
        return replyTimeoutInNs;
    }

    void conclude()
    {
        if (isConcluded.compareAndSet(false, true))
        {
            if (idleStrategy() == null)
            {
                idleStrategy(backoffIdleStrategy());
            }

            if (epochNanoClock() == null)
            {
                epochNanoClock(new OffsetEpochNanoClock());
            }
        }
        else
        {
            throw new IllegalStateException(
                "This configuration has already been concluded, are you trying to re-use it?");
        }
    }
}
