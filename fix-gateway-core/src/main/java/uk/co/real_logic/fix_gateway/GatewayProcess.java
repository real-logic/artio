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
import uk.co.real_logic.fix_gateway.replication.ReplicatedStream;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import java.nio.channels.ClosedByInterruptException;

public class GatewayProcess implements AutoCloseable
{
    public static final int INBOUND_DATA_STREAM = 0;
    public static final int INBOUND_CONTROL_STREAM = 1;
    public static final int OUTBOUND_DATA_STREAM = 2;
    public static final int OUTBOUND_CONTROL_STREAM = 3;

    protected final StaticConfiguration configuration;

    protected MonitoringFile monitoringFile;
    protected FixCounters fixCounters;
    protected ErrorBuffer errorBuffer;
    protected Aeron aeron;
    protected ReplicatedStream inboundStreams;
    protected ReplicatedStream outboundStreams;

    protected GatewayProcess(final StaticConfiguration configuration)
    {
        this.configuration = configuration;
        initMonitoring(configuration);
        initAeron();
        initReplicationStreams(configuration);
    }

    private void initMonitoring(final StaticConfiguration configuration)
    {
        monitoringFile = new MonitoringFile(true, configuration);
        fixCounters = new FixCounters(monitoringFile.createCountersManager());
        final MilliClock clock = System::currentTimeMillis;
        errorBuffer = new ErrorBuffer(monitoringFile.errorBuffer(), fixCounters.exceptions(), clock);
    }

    private void initReplicationStreams(final StaticConfiguration configuration)
    {
        final String channel = configuration.aeronChannel();

        inboundStreams = new ReplicatedStream(
            channel, aeron, fixCounters.failedInboundPublications(), INBOUND_DATA_STREAM, INBOUND_CONTROL_STREAM);
        outboundStreams = new ReplicatedStream(
            channel, aeron, fixCounters.failedOutboundPublications(), OUTBOUND_DATA_STREAM, OUTBOUND_CONTROL_STREAM);
    }

    private void initAeron()
    {
        final Aeron.Context ctx = new Aeron.Context();
        ctx.errorHandler(throwable ->
        {
            if (!(throwable instanceof ClosedByInterruptException))
            {
                errorBuffer.onError(throwable);
            }
        });
        aeron = Aeron.connect(ctx);
    }

    public void close()
    {
        inboundStreams.close();
        outboundStreams.close();
        aeron.close();
        monitoringFile.close();
    }
}
