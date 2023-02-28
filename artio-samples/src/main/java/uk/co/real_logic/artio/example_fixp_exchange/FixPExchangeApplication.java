/*
 * Copyright 2015-2023 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.example_fixp_exchange;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import org.agrona.IoUtil;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.SampleUtil;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.messages.FixPProtocolType;

import java.io.File;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.driver.ThreadingMode.SHARED;
import static uk.co.real_logic.artio.CommonConfiguration.backoffIdleStrategy;


/**
 * You can use the class FixPExchangeExampleBuyer in order to test out this sample.
 */
public final class FixPExchangeApplication
{
    public static void main(final String[] args) throws Exception
    {
        // Static configuration lasts the duration of a FIX-Gateway instance
        final EngineConfiguration configuration = new EngineConfiguration()
            .bindTo("localhost", 9999)
            .libraryAeronChannel(IPC_CHANNEL)
            .logFileDir("exchange-application")

            // Tell Artio which FIXP protocol you are going to accept
            .acceptFixPProtocol(FixPProtocolType.BINARY_ENTRYPOINT)

            // Configure the FIXP Authentication strategy
            .fixPAuthenticationStrategy((context, authProxy) ->
            {
                System.out.println("Request to authenticate: " + context);
                authProxy.accept();
            });

        cleanupOldLogFileDir(configuration);

        final MediaDriver.Context context = new MediaDriver.Context()
            .threadingMode(SHARED)
            .sharedIdleStrategy(backoffIdleStrategy())
            .dirDeleteOnStart(true);

        final Archive.Context archiveContext = new Archive.Context()
            .threadingMode(ArchiveThreadingMode.SHARED)
            .idleStrategySupplier(CommonConfiguration::backoffIdleStrategy)
            .deleteArchiveOnStart(true);

        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(context, archiveContext);
            FixEngine gateway = FixEngine.launch(configuration))
        {
            SampleUtil.runAgentUntilSignal(new FixPExchangeAgent(), driver.mediaDriver());
        }

        System.exit(0);
    }

    public static void cleanupOldLogFileDir(final EngineConfiguration configuration)
    {
        IoUtil.delete(new File(configuration.logFileDir()), true);
    }
}
