/*
 * Copyright 2015-2023 Real Logic Limited.
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
package uk.co.real_logic.artio.stress;

import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingIdleStrategy;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.SampleUtil;
import uk.co.real_logic.artio.client.TestReqIdFinder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.session.Session;

import java.util.Random;
import java.util.concurrent.CyclicBarrier;

import static java.util.Collections.singletonList;
import static org.agrona.SystemUtil.loadPropertiesFiles;
import static uk.co.real_logic.artio.messages.SessionState.DISCONNECTED;
import static uk.co.real_logic.artio.stress.StressConfiguration.MESSAGES_EXCHANGED;
import static uk.co.real_logic.artio.stress.StressConfiguration.NUM_SESSIONS;

public final class ConcurrentConnections
{
    public static void main(final String[] args) throws Exception
    {
        loadPropertiesFiles(args);

        final AgentRunner server = Server.createServer(new SleepingIdleStrategy(100), Throwable::printStackTrace);

        AgentRunner.startOnThread(server);

        final String aeronChannel = "aeron:udp?endpoint=localhost:10002";
        final EngineConfiguration engineConfiguration = new EngineConfiguration()
            .libraryAeronChannel(aeronChannel)
            .logFileDir("stress-client-logs");
        engineConfiguration.authenticationStrategy((logon) -> true);

        System.out.println("Client Logs at " + engineConfiguration.logFileDir());

        StressUtil.cleanupOldLogFileDir(engineConfiguration);

        final Random random = new Random(StressConfiguration.SEED);

        final long startTime = System.currentTimeMillis();

        try (FixEngine ignore = FixEngine.launch(engineConfiguration))
        {
            final CyclicBarrier barrier = new CyclicBarrier(NUM_SESSIONS);
            final Thread[] threads = new Thread[NUM_SESSIONS];

            for (int i = 0; i < NUM_SESSIONS; i++)
            {
                System.out.format("Starting session %d / %d%n", i + 1, NUM_SESSIONS);

                final int id = i;
                threads[i] = new Thread(
                    () -> runThread(aeronChannel, random, barrier, id));

                threads[i].start();
            }

            for (final Thread thread : threads)
            {
                thread.join();
            }
        }

        server.close();

        System.out.format("Sessions %d. Messages %d per session.%n", NUM_SESSIONS, MESSAGES_EXCHANGED);
        System.out.format("Stress test executed in %dms%n", System.currentTimeMillis() - startTime);
        System.exit(0);
    }

    private static void runThread(
        final String aeronChannel,
        final Random random,
        final CyclicBarrier barrier,
        final int id)
    {
        final String targetCompId = StressConfiguration.ACCEPTOR_ID + "-" + id;
        final String senderCompId = StressConfiguration.INITIATOR_ID + "-" + id;
        final String[] messagePool = StressUtil.constructMessagePool(String.valueOf(id), random);

        final TestReqIdFinder testReqIdFinder = new TestReqIdFinder();

        final SessionConfiguration sessionConfiguration = SessionConfiguration.builder()
            .address("localhost", StressConfiguration.PORT)
            .targetCompId(targetCompId)
            .senderCompId(senderCompId)
            .build();

        final LibraryConfiguration libraryConfiguration = new LibraryConfiguration();
        libraryConfiguration
            .sessionAcquireHandler((session, acquiredInfo) -> testReqIdFinder)
            .libraryAeronChannels(singletonList(aeronChannel));

        try (FixLibrary library = SampleUtil.blockingConnect(libraryConfiguration))
        {
            barrier.await();

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

            final Session session = reply.resultIfPresent();

            if (StressConfiguration.PRINT_EXCHANGE)
            {
                System.out.println(session.id() + " Replied with: " + reply.state());
            }

            while (!session.isActive())
            {
                idleStrategy.idle(library.poll(1));
            }

            StressUtil.exchangeMessages(
                library, session, idleStrategy, testReqIdFinder, messagePool, random, senderCompId);

            session.startLogout();
            session.requestDisconnect();
            while (session.state() != DISCONNECTED)
            {
                idleStrategy.idle(library.poll(1));
            }

            if (StressConfiguration.PRINT_EXCHANGE)
            {
                System.out.println(session.id() + " Disconnected");
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }
}
