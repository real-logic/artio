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
package uk.co.real_logic.fix_gateway.system_benchmarks;

import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.MessageValidationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.SenderCompIdValidationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.TargetCompIdValidationStrategy;

import java.io.File;
import java.util.Arrays;

import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;
import static uk.co.real_logic.fix_gateway.system_benchmarks.Configuration.ACCEPTOR_ID;
import static uk.co.real_logic.fix_gateway.system_benchmarks.Configuration.INITIATOR_ID;

public final class FixBenchmarkServer
{
    private static final int AERON_PORT = Integer.getInteger("fix.benchmark.aeron_port", 9998);

    public static void main(String[] args)
    {
        final EngineConfiguration configuration = engineConfiguration();

        try (final MediaDriver mediaDriver = newMediaDriver();
             final FixEngine engine = FixEngine.launch(configuration);
             final FixLibrary library = new FixLibrary(libraryConfiguration()))
        {
            final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1, 1 << 20);
            while (true)
            {
                idleStrategy.idle(library.poll(1));
            }
        }
    }

    private static MediaDriver newMediaDriver()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .threadingMode(SHARED)
            .dirsDeleteOnStart(true);

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
        return new EngineConfiguration()
            .bind("localhost", Configuration.PORT)
            .aeronChannel("udp://localhost:" + AERON_PORT)
            .logFileDir(acceptorLogs)
            .logInboundMessages(false)
            .logOutboundMessages(false);
    }

    private static LibraryConfiguration libraryConfiguration()
    {
        final MessageValidationStrategy validationStrategy =
            new TargetCompIdValidationStrategy(ACCEPTOR_ID)
                .and(new SenderCompIdValidationStrategy(Arrays.asList(INITIATOR_ID)));

        final AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.of(validationStrategy);

        return new LibraryConfiguration()
            .aeronChannel("udp://localhost:" + AERON_PORT)
            .authenticationStrategy(authenticationStrategy)
            .newSessionHandler(session -> new BenchmarkSessionHandler());
    }
}
