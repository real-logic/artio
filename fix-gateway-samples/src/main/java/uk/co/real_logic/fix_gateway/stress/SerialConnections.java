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
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import uk.co.real_logic.client.TestReqIdFinder;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.session.Session;

import java.util.Random;

import static java.util.Collections.singletonList;
import static uk.co.real_logic.fix_gateway.messages.SessionState.DISCONNECTED;
import static uk.co.real_logic.fix_gateway.stress.StressConfiguration.MAX_LENGTH;
import static uk.co.real_logic.fix_gateway.stress.StressConfiguration.MIN_LENGTH;

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
            .bindTo("localhost", 10001);

        System.out.println("Client Logs at " + engineConfiguration.logFileDir());

        Server.cleanupOldLogFileDir(engineConfiguration);

        final Random random = new Random(StressConfiguration.SEED);
        final byte[] messageContent = new byte[MAX_LENGTH + 1];

        for (int i = 0; i < messageContent.length; i++)
        {
            messageContent[i] = 'X';
        }

        try (final FixEngine fixEngine = FixEngine.launch(engineConfiguration))
        {
            for (int i = 0; i < StressConfiguration.NUM_SESSIONS; i++)
            {
                final TestReqIdFinder testReqIdFinder = new TestReqIdFinder();

                final SessionConfiguration sessionConfiguration = SessionConfiguration.builder()
                    .address("localhost", StressConfiguration.PORT)
                    .targetCompId(StressConfiguration.ACCEPTOR_ID)
                    .senderCompId(StressConfiguration.INITIATOR_ID)
                    .build();

                final LibraryConfiguration libraryConfiguration = new LibraryConfiguration()
                    .sessionAcquireHandler(session -> testReqIdFinder)
                    .libraryAeronChannels(singletonList(aeronChannel));

                try (final FixLibrary library = FixLibrary.connect(libraryConfiguration))
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

                    exchangeMessages(library, session, idleStrategy, testReqIdFinder, random, messageContent);

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

        System.exit(0);
    }

    private static String createMessage(final long sessionNum, final int messageNum, final String content)
    {
        return String.format("TestReqId-%d-%d-%s", sessionNum, messageNum, content);
    }

    private static void exchangeMessages(
        final FixLibrary library,
        final Session session,
        final IdleStrategy idleStrategy,
        final TestReqIdFinder testReqIdFinder,
        final Random random,
        final byte[] messageContent)
    {
        final TestRequestEncoder testRequest = new TestRequestEncoder();

        for (int j = 0; j < StressConfiguration.MESSAGES_EXCHANGED; j++)
        {
            final int messageLength = MIN_LENGTH + random.nextInt(MAX_LENGTH - MIN_LENGTH + 1);
            final String msg = createMessage(session.id(), j, new String(messageContent, 0, messageLength));
            testRequest.testReqID(msg);

            while (session.send(testRequest) < 0)
            {
                idleStrategy.idle(library.poll(1));
            }

            while (!msg.equals(testReqIdFinder.testReqId()))
            {
                idleStrategy.idle(library.poll(1));
            }

            if (StressConfiguration.PRINT_EXCHANGE)
            {
                System.out.println("Success, received reply!");
                System.out.println(testReqIdFinder.testReqId());
            }
        }
    }
}
