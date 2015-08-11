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
package uk.co.real_logic.server;

import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.concurrent.SigInt;
import uk.co.real_logic.fix_gateway.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.auth.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.auth.SenderCompIdAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.auth.TargetCompIdAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.session.Session;
import uk.co.real_logic.fix_gateway.library.session.SessionHandler;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;
import static uk.co.real_logic.fix_gateway.library.session.SessionState.DISCONNECTED;

public final class SampleServer
{

    public static final String ACCEPTOR_COMP_ID = "acceptor";
    public static final String INITIATOR_COMP_ID = "initiator";
    private static Session session;

    public static void main(final String[] args) throws Exception
    {
        final AuthenticationStrategy authenticationStrategy = new TargetCompIdAuthenticationStrategy(ACCEPTOR_COMP_ID)
            .and(new SenderCompIdAuthenticationStrategy(Arrays.asList(INITIATOR_COMP_ID)));

        // Static configuration lasts the duration of a FIX-Gateway instance
        final EngineConfiguration configuration = new EngineConfiguration()
            .bind("localhost", 9999)
            .aeronChannel("udp://localhost:10000");

        try (final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context().threadingMode(SHARED));
             final FixEngine gateway = FixEngine.launch(configuration))
        {

            final FixLibrary library = new FixLibrary(new LibraryConfiguration()
                // You register the new session handler - which is your application hook
                // that receives messages for new sessions
                .authenticationStrategy(authenticationStrategy)
                .newSessionHandler(SampleServer::onConnect));

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

        System.exit(0);
    }

    private static SessionHandler onConnect(final Session session)
    {
        SampleServer.session = session;

        // Simple server just handles a single connection on a single thread
        // You choose how to manage threads for your application.

        return new SampleSessionHandler(session);
    }

}
