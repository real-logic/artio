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
package uk.co.real_logic.fix_gateway;

import io.aeron.Aeron;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.*;
import org.agrona.concurrent.errors.DistinctErrorLog;
import uk.co.real_logic.fix_gateway.timing.HistogramLogAgent;
import uk.co.real_logic.fix_gateway.timing.Timer;

import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static io.aeron.driver.Configuration.ERROR_BUFFER_LENGTH_PROP_NAME;
import static org.agrona.concurrent.AgentRunner.startOnThread;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.TIME_MESSAGES;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.backoffIdleStrategy;
import static uk.co.real_logic.fix_gateway.dictionary.generation.Exceptions.closeAll;

public class GatewayProcess implements AutoCloseable
{
    public static final int INBOUND_LIBRARY_STREAM = 1;
    public static final int OUTBOUND_LIBRARY_STREAM = 2;
    public static final int OUTBOUND_REPLAY_STREAM = 3;

    protected CommonConfiguration configuration;
    protected MonitoringFile monitoringFile;
    protected FixCounters fixCounters;
    protected ErrorHandler errorHandler;
    protected DistinctErrorLog distinctErrorLog;
    protected Aeron aeron;
    protected AgentRunner monitoringRunner;

    protected void init(final CommonConfiguration configuration)
    {
        this.configuration = configuration;
        initMonitoring(configuration);
        initAeron(configuration);
    }

    private void initMonitoring(final CommonConfiguration configuration)
    {
        monitoringFile = new MonitoringFile(true, configuration);
        fixCounters = new FixCounters(monitoringFile.createCountersManager());
        final EpochClock clock = new SystemEpochClock();
        distinctErrorLog = new DistinctErrorLog(monitoringFile.errorBuffer(), clock);
        errorHandler = (throwable) ->
        {
            if (!distinctErrorLog.record(throwable))
            {
                System.err.println("Error Log is full, consider increasing " + ERROR_BUFFER_LENGTH_PROP_NAME);
                throwable.printStackTrace();
            }
        };
    }

    private void initAeron(final CommonConfiguration configuration)
    {
        final Aeron.Context ctx = aeronContext(configuration);
        aeron = Aeron.connect(ctx);
        CloseChecker.onOpen(ctx.aeronDirectoryName(), aeron);
    }

    private Aeron.Context aeronContext(final CommonConfiguration configuration)
    {
        final Aeron.Context ctx = configuration.aeronContext();
        ctx.imageMapMode(FileChannel.MapMode.READ_WRITE);
        ctx.errorHandler((throwable) ->
        {
            if (!(throwable instanceof ClosedByInterruptException))
            {
                errorHandler.onError(throwable);
            }
        });

        return ctx;
    }

    protected void start()
    {
        if (monitoringRunner != null)
        {
            startOnThread(monitoringRunner);
        }
    }

    protected void initMonitoringAgent(final List<Timer> timers, final CommonConfiguration configuration)
    {
        final List<Agent> agents = new ArrayList<>();
        if (TIME_MESSAGES)
        {
            agents.add(new HistogramLogAgent(
                timers,
                configuration.histogramLoggingFile(),
                configuration.histogramPollPeriodInMs(),
                errorHandler,
                new SystemEpochClock(),
                configuration.histogramHandler(),
                configuration.agentNamePrefix()));
        }

        if (configuration.printErrorMessages())
        {
            agents.add(new ErrorPrinter(monitoringFile.errorBuffer(), configuration.agentNamePrefix()));
        }

        if (!agents.isEmpty())
        {
            monitoringRunner = new AgentRunner(
                backoffIdleStrategy(), errorHandler, null, new CompositeAgent(agents));
        }
    }

    public void close()
    {
        closeAll(
            monitoringRunner,
            () ->
            {
                aeron.close();
                // Only record this as closed if the aeron.close() succeeded.
                CloseChecker.onClose(configuration.aeronContext().aeronDirectoryName(), aeron);
            },
            monitoringFile);
    }
}
