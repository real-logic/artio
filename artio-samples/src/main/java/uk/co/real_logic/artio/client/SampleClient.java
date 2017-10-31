/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.client;

import org.agrona.concurrent.SleepingIdleStrategy;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.SampleUtil;
import uk.co.real_logic.artio.builder.TestRequestEncoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.session.Session;

import java.io.File;

import static java.util.Collections.singletonList;
import static uk.co.real_logic.artio.CommonConfiguration.optimalTmpDirName;
import static uk.co.real_logic.artio.messages.SessionState.DISCONNECTED;
import static uk.co.real_logic.artio.server.SampleServer.*;

public final class SampleClient
{
    private static final TestReqIdFinder TEST_REQ_ID_FINDER = new TestReqIdFinder();

    public static void main(final String[] args) throws Exception
    {
        // Static configuration lasts the duration of a FIX-Gateway instance
        final String aeronChannel = "aeron:udp?endpoint=localhost:10002";
        final EngineConfiguration configuration = new EngineConfiguration()
            .libraryAeronChannel(aeronChannel)
            .monitoringFile(optimalTmpDirName() + File.separator + "fix-client" + File.separator + "engineCounters")
            .logFileDir("client-logs");

        cleanupOldLogFileDir(configuration);

        try (FixEngine ignore = FixEngine.launch(configuration))
        {
            // Each outbound session with an Exchange or broker is represented by
            // a Session object. Each session object can be configured with connection
            // details and credentials.
            final SessionConfiguration sessionConfig = SessionConfiguration.builder()
                .address("localhost", 9999)
                .targetCompId(ACCEPTOR_COMP_ID)
                .senderCompId(INITIATOR_COMP_ID)
                .build();

            try (FixLibrary library = SampleUtil.blockingConnect(new LibraryConfiguration()
                    .sessionAcquireHandler(SampleClient::onConnect)
                    .libraryAeronChannels(singletonList(aeronChannel))))
            {
                final SleepingIdleStrategy idleStrategy = new SleepingIdleStrategy(100);
                final Reply<Session> reply = library.initiate(sessionConfig);

                while (reply.isExecuting())
                {
                    idleStrategy.idle(library.poll(1));
                }

                if (!reply.hasCompleted())
                {
                    System.err.println(
                        "Unable to initiate the session, " + reply.state() + " " + reply.error());
                    System.exit(-1);
                }

                System.out.println("Replied with: " + reply.state());
                final Session session = reply.resultIfPresent();

                while (!session.canSendMessage())
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

    private static SessionHandler onConnect(final Session session, final boolean isSlow)
    {
        return TEST_REQ_ID_FINDER;
    }
}
