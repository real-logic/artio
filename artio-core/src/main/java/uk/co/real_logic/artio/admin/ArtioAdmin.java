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
import io.aeron.Counter;
import io.aeron.Subscription;
import io.aeron.exceptions.TimeoutException;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static uk.co.real_logic.artio.FixCounters.FixCountersId.FAILED_ADMIN_TYPE_ID;

/**
 * Provides a blocking wrapper for Artio API operations that can be used from a different process.
 *
 * This can be used for integration into commandline tools (such as FixAdminTool) or external processes, such as
 * web services APIs or custom GUI tools. It is designed to provide functionality for tasks that operational personnel
 * may want to perform on a FIX Gateway.
 */
public final class ArtioAdmin implements AutoCloseable
{
    private static final int MAX_CLAIM_ATTEMPTS = 10_000;
    private static final int FRAGMENT_LIMIT = 10;

    private final Lock lock = new ReentrantLock();
    private final AdminEndPointHandler handler = new AdminEndPointHandler();
    private final AdminApiProtocolSubscription protocolSubscription = new AdminApiProtocolSubscription(handler);

    private final AdminPublication outboundPublication;
    private final Subscription inboundSubscription;
    private final Aeron aeron;
    private final IdleStrategy idleStrategy;
    private final EpochNanoClock epochNanoClock;
    private final Counter failCounter;
    private final long replyTimeoutInNs;
    private final BooleanSupplier checkReplyFunc = this::checkReply;
    private final BooleanSupplier saveRequestAllFixSessionsFunc = this::saveRequestAllFixSessionsFunc;
    private final Supplier<List<FixAdminSession>> allFixSessionsResultFunc = handler::allFixSessions;

    private volatile boolean closed = false;

    private long correlationId;

    public static ArtioAdmin launch(final ArtioAdminConfiguration config)
    {
        return new ArtioAdmin(config);
    }

    private ArtioAdmin(final ArtioAdminConfiguration config)
    {
        config.conclude();

        aeron = Aeron.connect(config.aeronContext());
        idleStrategy = config.idleStrategy();
        epochNanoClock = config.epochNanoClock();
        failCounter = aeron.addCounter(FAILED_ADMIN_TYPE_ID.id(), "Failed offer for admin publication");
        replyTimeoutInNs = config.replyTimeoutInNs();

        final String channel = config.libraryAeronChannel();
        outboundPublication = new AdminPublication(
            aeron.addExclusivePublication(channel, config.outboundAdminStream()),
            failCounter,
            idleStrategy,
            MAX_CLAIM_ATTEMPTS);
        inboundSubscription = aeron.addSubscription(channel, config.inboundAdminStream());
    }

    public List<FixAdminSession> allFixSessions()
    {
        return exchangeMessage(saveRequestAllFixSessionsFunc, allFixSessionsResultFunc);
    }

    private boolean saveRequestAllFixSessionsFunc()
    {
        return outboundPublication.saveRequestAllFixSessions(correlationId) > 0;
    }

    public void disconnectSession(final long sessionId)
    {
        exchangeMessage(
            () -> outboundPublication.saveDisconnectSession(correlationId, sessionId) > 0,
            handler::checkError);
    }

    private <T> T exchangeMessage(final BooleanSupplier sendMessage, final Supplier<T> getResult)
    {
        lock.lock();
        try
        {
            checkOpen();

            final long deadlineInNs = nanoTime() + replyTimeoutInNs;
            correlationId = newCorrelationId();
            handler.expectedCorrelationId(correlationId);
            until(deadlineInNs, sendMessage);
            until(deadlineInNs, checkReplyFunc);

            return getResult.get();
        }
        finally
        {
            lock.unlock();
        }
    }

    private boolean checkReply()
    {
        inboundSubscription.poll(protocolSubscription, FRAGMENT_LIMIT);
        return handler.hasReceivedReply();
    }

    private long newCorrelationId()
    {
        return ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
    }

    private void until(final long deadlineInNs, final BooleanSupplier attempt)
    {
        while (true)
        {
            if (nanoTime() > deadlineInNs)
            {
                throw new TimeoutException("Timeout");
            }

            if (attempt.getAsBoolean())
            {
                break;
            }

            idleStrategy.idle();
        }
    }

    private long nanoTime()
    {
        return epochNanoClock.nanoTime();
    }

    private void checkOpen()
    {
        if (closed)
        {
            throw new IllegalStateException("client is closed");
        }
    }

    public void close()
    {
        lock.lock();
        try
        {
            if (!closed)
            {
                Exceptions.closeAll(failCounter, aeron);
                closed = true;
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    public boolean isClosed()
    {
        return closed;
    }
}
