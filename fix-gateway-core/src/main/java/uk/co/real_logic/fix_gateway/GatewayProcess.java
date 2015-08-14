/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.fix_gateway.streams.Streams;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import java.nio.channels.ClosedByInterruptException;

public class GatewayProcess implements AutoCloseable
{
    public static final int INBOUND_LIBRARY_STREAM = 0;
    public static final int OUTBOUND_LIBRARY_STREAM = 1;
    public static final int OUTBOUND_REPLAY_STREAM = 2;

    protected MonitoringFile monitoringFile;
    protected FixCounters fixCounters;
    protected ErrorBuffer errorBuffer;
    protected Aeron aeron;
    protected Streams inboundLibraryStreams;
    protected Streams outboundLibraryStreams;

    protected GatewayProcess(final CommonConfiguration configuration)
    {
        initMonitoring(configuration);
        initAeron(configuration);
        initStreams(configuration);
    }

    private void initMonitoring(final CommonConfiguration configuration)
    {
        monitoringFile = new MonitoringFile(true, configuration);
        fixCounters = new FixCounters(monitoringFile.createCountersManager());
        final MilliClock clock = System::currentTimeMillis;
        errorBuffer = new ErrorBuffer(monitoringFile.errorBuffer(), fixCounters.exceptions(), clock);
    }

    private void initStreams(final CommonConfiguration configuration)
    {
        final String channel = configuration.aeronChannel();

        inboundLibraryStreams = new Streams(
            channel, aeron, fixCounters.failedInboundPublications(), INBOUND_LIBRARY_STREAM);
        outboundLibraryStreams = new Streams(
            channel, aeron, fixCounters.failedOutboundPublications(), OUTBOUND_LIBRARY_STREAM);
    }

    private void initAeron(final CommonConfiguration configuration)
    {
        final Aeron.Context ctx = aeronContext(configuration);
        aeron = Aeron.connect(ctx);
    }

    protected BackoffIdleStrategy backoffIdleStrategy()
    {
        return new BackoffIdleStrategy(1, 1, 1, 1 << 20);
    }

    protected Aeron.Context aeronContext(final CommonConfiguration configuration)
    {
        final Aeron.Context ctx = new Aeron.Context();
        ctx.errorHandler(throwable ->
        {
            if (!(throwable instanceof ClosedByInterruptException))
            {
                errorBuffer.onError(throwable);
            }
        });
        return ctx;
    }

    public void close()
    {
        inboundLibraryStreams.close();
        outboundLibraryStreams.close();
        aeron.close();
        monitoringFile.close();
    }
}
