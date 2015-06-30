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
import uk.co.real_logic.fix_gateway.FixEngine;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.auth.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.auth.CompIdAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.auth.SenderIdAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.session.SessionHandler;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;

public final class SampleServer
{

    public static final String ACCEPTOR_COMP_ID = "acceptor";
    public static final String INITIATOR_COMP_ID = "initiator";

    private static SampleSessionHandler sessionHandler;

    public static void main(final String[] args) throws Exception
    {
        final AuthenticationStrategy authenticationStrategy = new CompIdAuthenticationStrategy(ACCEPTOR_COMP_ID)
            .and(new SenderIdAuthenticationStrategy(Arrays.asList(INITIATOR_COMP_ID)));

        // Static configuration lasts the duration of a FIX-Gateway instance
        final StaticConfiguration configuration = new StaticConfiguration()
            .bind("localhost", 9999)
            .aeronChannel("udp://localhost:10000")
            .authenticationStrategy(authenticationStrategy)

            // You register the new session handler - which is your application hook
            // that receives messages for new sessions
            .newSessionHandler(SampleServer::onConnect);

        try (final MediaDriver driver = MediaDriver.launch(new MediaDriver.Context().threadingMode(SHARED));
             final FixEngine gateway = FixEngine.launch(configuration))
        {
            // This would be the same as the SampleOtfMain sample code for sending a message.

            final AtomicBoolean running = new AtomicBoolean(true);
            SigInt.register(() -> running.set(false));

            while (running.get())
            {
                synchronized (SampleServer.class)
                {
                    if (sessionHandler != null)
                    {
                        sessionHandler.run();
                    }
                }

                Thread.sleep(100);
            }
        }
    }

    private static synchronized SessionHandler onConnect(final Session session)
    {
        // Simple server just handles a single connection on a single thread
        // You choose how to manage threads for your application.

        sessionHandler = new SampleSessionHandler(session, null);
        return sessionHandler;
    }

}
