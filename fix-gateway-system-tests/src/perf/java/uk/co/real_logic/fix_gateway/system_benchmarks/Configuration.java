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
package uk.co.real_logic.fix_gateway.system_benchmarks;

import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;
import uk.co.real_logic.agrona.concurrent.YieldingIdleStrategy;

import static uk.co.real_logic.aeron.CommonContext.IPC_CHANNEL;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.backoffIdleStrategy;

public final class Configuration
{
    public static final int PORT = Integer.getInteger("fix.benchmark.port", 9999);
    public static final String AERON_CHANNEL = System.getProperty("fix.benchmark.aeron_channel", IPC_CHANNEL);
    public static final String ACCEPTOR_ID = "ACC";
    public static final String INITIATOR_ID = "INIT";
    public static final String TYPE = System.getProperty("fix.benchmark.type", "latency");
    public static final boolean LOG_INBOUND_MESSAGES = Boolean.getBoolean("fix.benchmark.log_in");
    public static final boolean LOG_OUTBOUND_MESSAGES = Boolean.getBoolean("fix.benchmark.log_out");
    public static final IdleStrategy IDLE_STRATEGY = idleStrategy("fix.benchmark.engine_idle");

    private static IdleStrategy idleStrategy(final String propertyName)
    {
        final String strategyName = System.getProperty(propertyName, "");
        switch (strategyName)
        {
            case "noop":
                return new NoOpIdleStrategy();

            case "yield":
                return new YieldingIdleStrategy();

            case "backoff":
                return backoffIdleStrategy();

            default:
                return backoffIdleStrategy();
        }
    }
}
