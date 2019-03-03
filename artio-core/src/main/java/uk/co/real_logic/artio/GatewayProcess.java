/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio;

import io.aeron.Aeron;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.*;
import org.agrona.concurrent.errors.DistinctErrorLog;
import uk.co.real_logic.artio.timing.HistogramLogAgent;
import uk.co.real_logic.artio.timing.Timer;

import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.List;

import static io.aeron.driver.Configuration.ERROR_BUFFER_LENGTH_PROP_NAME;
import static uk.co.real_logic.artio.CommonConfiguration.TIME_MESSAGES;
import static uk.co.real_logic.artio.dictionary.generation.Exceptions.closeAll;

public class GatewayProcess implements AutoCloseable
{
    /** Common id used by messages in both engine and library */
    public static final long NO_CORRELATION_ID = 0;

    private static long startTimeInMs = System.currentTimeMillis();

    private DistinctErrorLog distinctErrorLog;

    protected CommonConfiguration configuration;
    protected MonitoringFile monitoringFile;
    protected FixCounters fixCounters;
    protected ErrorHandler errorHandler;
    protected Aeron aeron;
    protected Agent monitoringAgent;

    protected void init(final CommonConfiguration configuration)
    {
        this.configuration = configuration;
        initMonitoring(configuration);
        initAeron(configuration);
    }

    protected void initMonitoring(final CommonConfiguration configuration)
    {
        monitoringFile = new MonitoringFile(true, configuration);
        final EpochClock clock = new SystemEpochClock();
        distinctErrorLog = new DistinctErrorLog(monitoringFile.errorBuffer(), clock);
        errorHandler =
            (throwable) ->
            {
                if (!distinctErrorLog.record(throwable))
                {
                    System.err.println("Error Log is full, consider increasing " + ERROR_BUFFER_LENGTH_PROP_NAME);
                    throwable.printStackTrace();
                }
            };
    }

    protected void initAeron(final CommonConfiguration configuration)
    {
        final Aeron.Context context = configureAeronContext(configuration);
        aeron = Aeron.connect(context);
        CloseChecker.onOpen(context.aeronDirectoryName(), aeron);
        fixCounters = new FixCounters(aeron);
    }

    public Agent conductorAgent()
    {
        final AgentInvoker invoker = aeron.conductorAgentInvoker();
        if (invoker == null)
        {
            return null;
        }

        return invoker.agent();
    }

    protected Aeron.Context configureAeronContext(final CommonConfiguration configuration)
    {
        final Aeron.Context ctx = configuration.aeronContext();
        ctx.errorHandler(
            (throwable) ->
            {
                if (!(throwable instanceof ClosedByInterruptException))
                {
                    errorHandler.onError(throwable);
                }
            });

        return ctx;
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
            agents.add(new ErrorPrinter(monitoringFile.errorBuffer(), configuration.agentNamePrefix(), startTimeInMs));
        }

        if (!agents.isEmpty())
        {
            monitoringAgent = new CompositeAgent(agents);
        }
    }

    public void close()
    {
        closeAll(
            fixCounters,
            () ->
            {
                aeron.close();
                // Only record this as closed if the aeron.close() succeeded.
                CloseChecker.onClose(configuration.aeronContext().aeronDirectoryName(), aeron);
            },
            monitoringFile);
    }
}
