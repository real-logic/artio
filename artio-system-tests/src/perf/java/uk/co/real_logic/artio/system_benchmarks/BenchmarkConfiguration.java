/*
 * Copyright 2015-2021 Real Logic Limited.
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
package uk.co.real_logic.artio.system_benchmarks;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static uk.co.real_logic.artio.CommonConfiguration.backoffIdleStrategy;

public final class BenchmarkConfiguration
{
    public static final int PORT = Integer.getInteger("fix.benchmark.port", 9999);
    public static final String AERON_CHANNEL = System.getProperty("fix.benchmark.aeron_channel", IPC_CHANNEL);
    public static final String ACCEPTOR_ID = "ACC";
    public static final String INITIATOR_ID = "INIT";
    public static final String TYPE = System.getProperty("fix.benchmark.type", "latency");
    public static final boolean LOG_INBOUND_MESSAGES = Boolean.getBoolean("fix.benchmark.log_in");
    public static final boolean LOG_OUTBOUND_MESSAGES = Boolean.getBoolean("fix.benchmark.log_out");
    public static final int WARMUP_MESSAGES = Integer.getInteger("fix.benchmark.warmup", 10_000);
    public static final int MESSAGES_EXCHANGED = Integer.getInteger("fix.benchmark.messages", 50_000);
    public static final boolean BAD_LOGON = Boolean.getBoolean("fix.benchmark.bad_logon");
    public static final int MAX_MESSAGES_IN_FLIGHT = Integer.getInteger("fix.benchmark.max_messages_in_flight", 20);
    public static final int SEND_RATE_PER_SECOND = Integer.getInteger("fix.benchmark.send_rate_sec", 1_000);
    public static final int NUMBER_OF_SESSIONS = Integer.getInteger("fix.benchmark.num_sessions", 25);
    public static final long LOGOUT_LINGER_TIMEOUT_IN_MS = Long.getLong(
        "fix.benchmark.logout_linger_timeout", TimeUnit.SECONDS.toMillis(2));
    public static final String VALID_PASSWORD = "password";
    public static final char[] VALID_PASSWORD_CHARS = VALID_PASSWORD.toCharArray();

    static IdleStrategy idleStrategy()
    {
        final String strategyName = System.getProperty("fix.benchmark.engine_idle", "");
        switch (strategyName)
        {
            case "noop":
                return new NoOpIdleStrategy();

            case "yield":
                return new YieldingIdleStrategy();

            default:
            case "backoff":
                return backoffIdleStrategy();
        }
    }
}
