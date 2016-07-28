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
package uk.co.real_logic.fix_gateway.protocol;

import io.aeron.logbuffer.BufferClaim;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.ReliefValve;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.replication.ClusterablePublication;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.Publication.CLOSED;
import static io.aeron.Publication.NOT_CONNECTED;

/**
 * A publication designed for deterministic claiming.
 */
class ClaimablePublication implements AutoCloseable
{
    public static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;

    protected final MessageHeaderEncoder header = new MessageHeaderEncoder();

    protected final BufferClaim bufferClaim;
    protected final ClusterablePublication dataPublication;
    protected final IdleStrategy idleStrategy;

    private final long maxClaimAttempts;
    private final ReliefValve reliefValve;
    private final AtomicCounter fails;

    ClaimablePublication(
        final int maxClaimAttempts,
        final IdleStrategy idleStrategy,
        final AtomicCounter fails,
        final ReliefValve reliefValve,
        final ClusterablePublication dataPublication)
    {
        this.maxClaimAttempts = maxClaimAttempts;
        this.idleStrategy = idleStrategy;
        this.fails = fails;
        bufferClaim = new BufferClaim();
        this.reliefValve = reliefValve;
        this.dataPublication = dataPublication;
    }

    protected long claim(final int framedLength)
    {
        return claim(framedLength, bufferClaim);
    }

    public long claim(final int framedLength, final BufferClaim bufferClaim)
    {
        long position = 0;
        long i = 0;
        do
        {
            position = dataPublication.tryClaim(framedLength, bufferClaim);

            if (position > 0L)
            {
                return position;
            }
            else if (position == BACK_PRESSURED)
            {
                idleStrategy.idle(reliefValve.vent());
            }
            else
            {
                idleStrategy.idle();
            }

            fails.increment();
            i++;
        } while (i <= maxClaimAttempts);

        idleStrategy.reset();

        if (position == NOT_CONNECTED || position == CLOSED)
        {
            throw new IllegalStateException(
                "Unable to send publish message, probably a missing an engine or library instance");
        }
        else
        {
            return position;
        }
    }

    public void close()
    {
        dataPublication.close();
    }
}
