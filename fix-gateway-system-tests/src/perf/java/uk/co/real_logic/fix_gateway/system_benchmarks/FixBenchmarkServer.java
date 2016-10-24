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

import io.aeron.driver.MediaDriver;
import org.agrona.IoUtil;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.CommonConfiguration;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.AcquiringSessionExistsHandler;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;

import java.io.File;

import static java.util.Collections.singletonList;
import static uk.co.real_logic.fix_gateway.system_benchmarks.BenchmarkConfiguration.*;

public final class FixBenchmarkServer
{
    public static void main(final String[] args)
    {
        final EngineConfiguration configuration = engineConfiguration();

        try (MediaDriver mediaDriver = newMediaDriver();
             FixEngine engine = FixEngine.launch(configuration);
             FixLibrary library = FixLibrary.connect(libraryConfiguration()))
        {
            final IdleStrategy idleStrategy = IDLE_STRATEGY;
            System.out.printf("Using %s idle strategy\n", idleStrategy.getClass().getSimpleName());
            while (true)
            {
                idleStrategy.idle(library.poll(10));
            }
        }
    }

    private static MediaDriver newMediaDriver()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .dirsDeleteOnStart(true)
            .publicationTermBufferLength(128 * 1024 * 1024)
            .maxTermBufferLength(128 * 1024 * 1024);

        return MediaDriver.launch(context);
    }

    private static EngineConfiguration engineConfiguration()
    {
        final String acceptorLogs = "acceptor_logs";
        final File dir = new File(acceptorLogs);
        if (dir.exists())
        {
            IoUtil.delete(dir, false);
        }

        final EngineConfiguration configuration = new EngineConfiguration();
        setupAuthentication(configuration);

        return configuration
            .bindTo("localhost", BenchmarkConfiguration.PORT)
            .libraryAeronChannel(AERON_CHANNEL)
            .logFileDir(acceptorLogs)
            .logInboundMessages(LOG_INBOUND_MESSAGES)
            .logOutboundMessages(LOG_OUTBOUND_MESSAGES)
            .framerIdleStrategy(IDLE_STRATEGY);
    }

    private static LibraryConfiguration libraryConfiguration()
    {
        final LibraryConfiguration configuration = new LibraryConfiguration();
        setupAuthentication(configuration);

        return configuration
            .libraryAeronChannels(singletonList(AERON_CHANNEL))
            .sessionAcquireHandler(session -> new BenchmarkSessionHandler())
            .sessionExistsHandler(new AcquiringSessionExistsHandler());
    }

    private static void setupAuthentication(final CommonConfiguration configuration)
    {
        configuration.authenticationStrategy((logon) -> !REJECT_LOGON);
    }
}
