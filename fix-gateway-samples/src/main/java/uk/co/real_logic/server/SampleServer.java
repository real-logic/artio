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
package uk.co.real_logic.server;

import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.MediaDriver.Context;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.SigInt;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.library.SessionHandler;
import uk.co.real_logic.fix_gateway.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.validation.MessageValidationStrategy;
import uk.co.real_logic.fix_gateway.validation.SenderCompIdValidationStrategy;
import uk.co.real_logic.fix_gateway.validation.TargetCompIdValidationStrategy;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;
import static uk.co.real_logic.fix_gateway.messages.SessionState.*;

public final class SampleServer
{

    public static final String ACCEPTOR_COMP_ID = "acceptor";
    public static final String INITIATOR_COMP_ID = "initiator";
    private static Session session;

    public static void main(final String[] args) throws Exception
    {
        final MessageValidationStrategy validationStrategy = new TargetCompIdValidationStrategy(ACCEPTOR_COMP_ID)
            .and(new SenderCompIdValidationStrategy(Arrays.asList(INITIATOR_COMP_ID)));

        final AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.of(validationStrategy);

        // Static configuration lasts the duration of a FIX-Gateway instance
        final String aeronChannel = "udp://localhost:10000";
        final EngineConfiguration configuration = new EngineConfiguration()
            .bindTo("localhost", 9999)
            .aeronChannel(aeronChannel);
        configuration.authenticationStrategy(authenticationStrategy);

        cleanupOldLogFileDir(configuration);

        final Context context = new Context()
            .threadingMode(SHARED)
            .dirsDeleteOnStart(true);
        try (final MediaDriver driver = MediaDriver.launch(context);
             final FixEngine gateway = FixEngine.launch(configuration))
        {
            final LibraryConfiguration libraryConfiguration = new LibraryConfiguration();
            libraryConfiguration.authenticationStrategy(authenticationStrategy);
            try (final FixLibrary library = FixLibrary.connect(libraryConfiguration
                // You register the new session handler - which is your application hook
                // that receives messages for new sessions
                .newSessionHandler(SampleServer::onConnect)
                .isAcceptor(true)
                .aeronChannel(aeronChannel)))
            {
                final AtomicBoolean running = new AtomicBoolean(true);
                SigInt.register(() -> running.set(false));

                while (running.get())
                {
                    library.poll(1);

                    if (session != null && session.state() == DISCONNECTED)
                    {
                        break;
                    }

                    Thread.sleep(100);
                }
            }
        }

        System.exit(0);
    }

    public static void cleanupOldLogFileDir(final EngineConfiguration configuration)
    {
        IoUtil.delete(new File(configuration.logFileDir()), true);
    }

    private static SessionHandler onConnect(final Session session)
    {
        SampleServer.session = session;

        // Simple server just handles a single connection on a single thread
        // You choose how to manage threads for your application.

        return new SampleSessionHandler(session);
    }

}
