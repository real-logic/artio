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
package uk.co.real_logic.fix_gateway.stress;

import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingIdleStrategy;
import uk.co.real_logic.fix_gateway.SampleUtil;
import uk.co.real_logic.fix_gateway.client.TestReqIdFinder;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.session.Session;

import java.util.Random;

import static java.util.Collections.singletonList;
import static uk.co.real_logic.fix_gateway.messages.SessionState.DISCONNECTED;
import static uk.co.real_logic.fix_gateway.stress.StressConfiguration.*;

public final class SerialConnections
{
    public static void main(final String[] args) throws Exception
    {
        MediaDriver.loadPropertiesFiles(args);

        final AgentRunner server = Server.createServer(new SleepingIdleStrategy(100), Throwable::printStackTrace);

        AgentRunner.startOnThread(server);

        final String aeronChannel = "aeron:udp?endpoint=localhost:10002";
        final EngineConfiguration engineConfiguration = new EngineConfiguration()
            .libraryAeronChannel(aeronChannel)
            .logFileDir("stress-client-logs")
            .bindTo("localhost", 10001);

        System.out.println("Client Logs at " + engineConfiguration.logFileDir());

        StressUtil.cleanupOldLogFileDir(engineConfiguration);

        final Random random = new Random(StressConfiguration.SEED);
        final String[] messagePool = StressUtil.constructMessagePool("", random);

        final long startTime = System.currentTimeMillis();

        try (FixEngine ignore = FixEngine.launch(engineConfiguration))
        {
            for (int i = 0; i < NUM_SESSIONS; i++)
            {
                System.out.format("Starting session %d / %d%n", i + 1, NUM_SESSIONS);

                final TestReqIdFinder testReqIdFinder = new TestReqIdFinder();

                final SessionConfiguration sessionConfiguration = SessionConfiguration.builder()
                    .address("localhost", StressConfiguration.PORT)
                    .targetCompId(ACCEPTOR_ID)
                    .senderCompId(INITIATOR_ID)
                    .build();

                final LibraryConfiguration libraryConfiguration = new LibraryConfiguration()
                    .sessionAcquireHandler(session -> testReqIdFinder)
                    .libraryAeronChannels(singletonList(aeronChannel));

                try (FixLibrary library = SampleUtil.blockingConnect(libraryConfiguration))
                {
                    final SleepingIdleStrategy idleStrategy = new SleepingIdleStrategy(100);
                    final Reply<Session> reply = library.initiate(sessionConfiguration);

                    while (reply.isExecuting())
                    {
                        idleStrategy.idle(library.poll(1));
                    }

                    if (!reply.hasCompleted())
                    {
                        System.err.println("Unable to initiate the session, " + reply.state());
                        reply.error().printStackTrace();
                        System.exit(-1);
                    }

                    if (StressConfiguration.PRINT_EXCHANGE)
                    {
                        System.out.println("Replied with: " + reply.state());
                    }

                    final Session session = reply.resultIfPresent();
                    while (!session.canSendMessage())
                    {
                        idleStrategy.idle(library.poll(1));
                    }

                    StressUtil.exchangeMessages(
                        library, session, idleStrategy, testReqIdFinder, messagePool, random, INITIATOR_ID);

                    session.startLogout();
                    session.requestDisconnect();
                    while (session.state() != DISCONNECTED)
                    {
                        idleStrategy.idle(library.poll(1));
                    }

                    if (StressConfiguration.PRINT_EXCHANGE)
                    {
                        System.out.println("Disconnected");
                    }
                }
            }
        }

        server.close();

        System.out.format("Sessions %d. Messages %d per session.%n", NUM_SESSIONS, MESSAGES_EXCHANGED);

        System.out.format("Stress test executed in %dms\n", System.currentTimeMillis() - startTime);
    }
}
