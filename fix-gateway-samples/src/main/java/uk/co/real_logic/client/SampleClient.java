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
package uk.co.real_logic.client;

import org.agrona.concurrent.SleepingIdleStrategy;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionHandler;
import uk.co.real_logic.fix_gateway.session.Session;

import static uk.co.real_logic.fix_gateway.messages.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.messages.SessionState.DISCONNECTED;
import static uk.co.real_logic.server.SampleServer.*;

public final class SampleClient
{
    private static final TestReqIdFinder TEST_REQ_ID_FINDER = new TestReqIdFinder();;

    public static void main(final String[] args) throws Exception
    {
        // Static configuration lasts the duration of a FIX-Gateway instance
        final String aeronChannel = "udp://localhost:10002";
        final EngineConfiguration configuration = new EngineConfiguration()
            .aeronChannel(aeronChannel)
            .bindTo("localhost", 10001);

        cleanupOldLogFileDir(configuration);

        try (final FixEngine gateway = FixEngine.launch(configuration))
        {
            // Each outbound session with an Exchange or broker is represented by
            // a Session object. Each session object can be configured with connection
            // details and credentials.
            final SessionConfiguration sessionConfig = SessionConfiguration.builder()
                .address("localhost", 9999)
                .targetCompId(ACCEPTOR_COMP_ID)
                .senderCompId(INITIATOR_COMP_ID)
                .build();

            try (final FixLibrary library = FixLibrary.connect(new LibraryConfiguration()
                .newSessionHandler(SampleClient::onConnect)
                .aeronChannel(aeronChannel)))
            {
                final SleepingIdleStrategy idleStrategy = new SleepingIdleStrategy(100);
                final Session session = library.initiate(sessionConfig);

                while (session.state() != ACTIVE)
                {
                    idleStrategy.idle(library.poll(1));
                }

                final TestRequestEncoder testRequest = new TestRequestEncoder();
                testRequest.testReqID("Hello World");

                session.send(testRequest);

                while (!"Hello World".equals(TEST_REQ_ID_FINDER.testReqId()))
                {
                    idleStrategy.idle(library.poll(1));
                }

                System.out.println("Success, received reply!");
                System.out.println(TEST_REQ_ID_FINDER.testReqId());

                session.startLogout();
                session.requestDisconnect();

                while (session.state() != DISCONNECTED)
                {
                    idleStrategy.idle(library.poll(1));
                }

                System.out.println("Disconnected");
            }
        }

        System.exit(0);
    }

    private static SessionHandler onConnect(final Session session)
    {
        return TEST_REQ_ID_FINDER;
    }
}
