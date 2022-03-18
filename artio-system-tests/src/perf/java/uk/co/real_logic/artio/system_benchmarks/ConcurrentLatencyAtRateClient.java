/*
 * Copyright 2022 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.system_benchmarks;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OffsetEpochNanoClock;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.*;
import uk.co.real_logic.artio.session.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.archive.client.AeronArchive.Configuration.CONTROL_CHANNEL_PROP_NAME;
import static io.aeron.archive.client.AeronArchive.Configuration.CONTROL_RESPONSE_CHANNEL_PROP_NAME;
import static io.aeron.driver.ThreadingMode.SHARED;
import static java.util.Collections.singletonList;
import static uk.co.real_logic.artio.system_benchmarks.BenchmarkConfiguration.*;
import static uk.co.real_logic.artio.system_benchmarks.ClientBenchmarkSessionHandler.isComplete;

public class ConcurrentLatencyAtRateClient
{
    private static final int INITIATE_STATE = 0;
    private static final int SEND_STATE = 1;
    private static final int WAIT_STATE = 2;

    public static final String AERON_DIRECTORY_NAME = "/dev/shm/benchmark-client";
    public static final String AERON_ARCHIVE_DIRECTORY_NAME = "benchmark-client-archive";
    public static final String RECORDING_EVENTS_CHANNEL = "aeron:udp?endpoint=localhost:9030";

    public static void main(final String[] args) throws InterruptedException
    {
        System.setProperty(CONTROL_CHANNEL_PROP_NAME, "aeron:udp?endpoint=localhost:9010");
        System.setProperty(CONTROL_RESPONSE_CHANNEL_PROP_NAME, "aeron:udp?endpoint=localhost:9020");

        final MediaDriver.Context context = new MediaDriver.Context()
            .threadingMode(SHARED)
            .dirDeleteOnStart(true)
            .aeronDirectoryName(AERON_DIRECTORY_NAME);

        final Archive.Context archiveContext = new Archive.Context()
            .threadingMode(ArchiveThreadingMode.SHARED)
            .deleteArchiveOnStart(true)
            .aeronDirectoryName(AERON_DIRECTORY_NAME)
            .recordingEventsChannel(RECORDING_EVENTS_CHANNEL)
            .archiveDirectoryName(AERON_ARCHIVE_DIRECTORY_NAME);

        final ClientBenchmarkSessionHandler[] sessionHandlers = new ClientBenchmarkSessionHandler[NUMBER_OF_SESSIONS];
        final EpochNanoClock nanoClock = new OffsetEpochNanoClock();

        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(context, archiveContext);
            FixEngine engine = FixEngine.launch(engineConfiguration(nanoClock));
            FixLibrary library = FixLibrary.connect(libraryConfiguration(sessionHandlers, nanoClock)))
        {
            final IdleStrategy idleStrategy = idleStrategy();
            System.out.printf("Using %s idle strategy%n", idleStrategy.getClass().getSimpleName());
            while (true)
            {
                final boolean notConnected = !library.isConnected();

                idleStrategy.idle(library.poll(10));

                if (notConnected && library.isConnected())
                {
                    break;
                }
            }

            final List<Reply<Session>> replies = initiateConnections(library);

            final Session[] sessions = new Session[NUMBER_OF_SESSIONS];

            runLoop(sessionHandlers, nanoClock, library, idleStrategy, replies, sessions);
        }
    }

    private static void runLoop(
        final ClientBenchmarkSessionHandler[] sessionHandlers,
        final EpochNanoClock nanoClock,
        final FixLibrary library,
        final IdleStrategy idleStrategy,
        final List<Reply<Session>> replies,
        final Session[] sessions)
    {
        final long messageSendTimeoutInNs = TimeUnit.SECONDS.toNanos(1) / SEND_RATE_PER_SECOND;
        System.out.println("messageSendTimeoutInNs = " + messageSendTimeoutInNs);

        int state = INITIATE_STATE;
        long nextMessageSendTimeInNs = 0;
        int sent = 0;

        while (true)
        {
            idleStrategy.idle(library.poll(10));

            switch (state)
            {
                case INITIATE_STATE:
                    state = awaitReplies(replies, sessions, sessionHandlers);
                    break;

                case SEND_STATE:
                    final long timeInNs = nanoClock.nanoTime();
                    if (timeInNs > nextMessageSendTimeInNs)
                    {
                        final ClientBenchmarkSessionHandler sessionHandler =
                            sessionHandlers[sent % NUMBER_OF_SESSIONS];
                        if (sessionHandler.trySend())
                        {
                            sent++;
                            nextMessageSendTimeInNs = timeInNs + messageSendTimeoutInNs;

                            if (sent >= MESSAGES_EXCHANGED)
                            {
                                state = WAIT_STATE;
                            }
                        }
                    }
                    break;

                case WAIT_STATE:
                    if (isComplete())
                    {
                        ClientBenchmarkSessionHandler.dumpLatencies();
                        return;
                    }
                    break;
            }
        }
    }

    private static int awaitReplies(
        final List<Reply<Session>> replies, final Session[] sessions,
        final ClientBenchmarkSessionHandler[] sessionHandlers)
    {
        for (int i = 0; i < NUMBER_OF_SESSIONS; i++)
        {
            final Reply<Session> reply = replies.get(i);
            if (sessions[i] != null)
            {
                if (reply.hasErrored())
                {
                    reply.error().printStackTrace();
                    System.exit(-1);
                }

                if (reply.hasTimedOut())
                {
                    System.err.println("Timed out: " + reply);
                    System.exit(-2);
                }

                if (reply.hasCompleted())
                {
                    sessions[i] = reply.resultIfPresent();
                }
            }
        }

        int connected = 0;
        for (final ClientBenchmarkSessionHandler sessionHandler : sessionHandlers)
        {
            if (sessionHandler != null && sessionHandler.isActive())
            {
                connected++;
            }
        }

        if (connected == NUMBER_OF_SESSIONS)
        {
            System.out.println("All sessions connected");
            return SEND_STATE;
        }

        return INITIATE_STATE;
    }

    private static List<Reply<Session>> initiateConnections(final FixLibrary library)
    {
        final List<Reply<Session>> connectionReplies = new ArrayList<>(NUMBER_OF_SESSIONS);
        for (int i = 0; i < NUMBER_OF_SESSIONS; i++)
        {
            final String senderCompId = INITIATOR_ID + i;
            connectionReplies.add(library.initiate(SessionConfiguration.builder()
                .address("localhost", PORT)
                .senderCompId(senderCompId)
                .targetCompId(ACCEPTOR_ID)
                .credentials(senderCompId, VALID_PASSWORD)
                .build()));
        }
        return connectionReplies;
    }

    private static EngineConfiguration engineConfiguration(final EpochNanoClock epochNanoClock)
    {
        final EngineConfiguration configuration = new EngineConfiguration();
        configuration.printAeronStreamIdentifiers(true);

        configuration.aeronContext().aeronDirectoryName(AERON_DIRECTORY_NAME);
        configuration.aeronArchiveContext()
            .aeronDirectoryName(AERON_DIRECTORY_NAME)
            .recordingEventsChannel(RECORDING_EVENTS_CHANNEL);

        return configuration
            .epochNanoClock(epochNanoClock)
            .libraryAeronChannel(AERON_CHANNEL)
            .deleteLogFileDirOnStart(true)
            .logFileDir("benchmark-client-logs")
            .logInboundMessages(LOG_INBOUND_MESSAGES)
            .logOutboundMessages(LOG_OUTBOUND_MESSAGES)
            .framerIdleStrategy(idleStrategy());
    }

    private static LibraryConfiguration libraryConfiguration(
        final ClientBenchmarkSessionHandler[] sessionHandlers, final EpochNanoClock epochNanoClock)
    {
        final LibraryConfiguration configuration = new LibraryConfiguration();
        configuration.printAeronStreamIdentifiers(true);

        return configuration
            .epochNanoClock(epochNanoClock)
            .libraryAeronChannels(singletonList(AERON_CHANNEL))
            .sessionAcquireHandler(new SessionAcquireHandler()
            {
                public int index = 0;

                public SessionHandler onSessionAcquired(final Session session, final SessionAcquiredInfo acquiredInfo)
                {
                    final ClientBenchmarkSessionHandler sessionHandler =
                        new ClientBenchmarkSessionHandler(session, epochNanoClock);
                    sessionHandlers[index] = sessionHandler;
                    index++;
                    return sessionHandler;
                }
            })
            .libraryConnectHandler(new LibraryConnectHandler()
            {
                public void onConnect(final FixLibrary library)
                {
                    System.out.println("Library: onConnect");
                }

                public void onDisconnect(final FixLibrary library)
                {
                    System.out.println("Library: onDisconnect");
                }
            });
    }
}
