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
package uk.co.real_logic.artio.system_tests;

import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.framer.LibraryInfo;
import uk.co.real_logic.artio.engine.logger.FixArchiveScanner;
import uk.co.real_logic.artio.engine.logger.FixMessageConsumer;
import uk.co.real_logic.artio.engine.logger.FixMessagePredicate;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.library.SessionConfiguration.Builder;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.session.Session;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.LogTag.GATEWAY_CLUSTER_TEST;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.Timing.withTimeout;
import static uk.co.real_logic.artio.Constants.TEST_REQUEST_MESSAGE;
import static uk.co.real_logic.artio.dictionary.generation.Exceptions.closeAll;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_SESSION_ID_FILE;
import static uk.co.real_logic.artio.engine.logger.FixArchiveScanner.MessageType.SENT;
import static uk.co.real_logic.artio.engine.logger.FixMessagePredicates.*;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

// TODO: re-enable once time and resources can be allocated to fix this kind of issue
@Ignore
public class ClusteredGatewaySystemTest
{
    private static final int CLUSTER_SIZE = 3;

    private static final int FRAGMENT_LIMIT = 10;

    private int libraryAeronPort = unusedPort();
    private List<FixEngineRunner> acceptingCluster;

    private FixLibrary acceptingLibrary;

    private MediaDriver initiatingMediaDriver;
    private FixEngine initiatingEngine;
    private FixLibrary initiatingLibrary;

    private FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler acceptingHandler = new FakeHandler(acceptingOtfAcceptor);

    private FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler initiatingHandler = new FakeHandler(initiatingOtfAcceptor);

    private Session initiatingSession;
    private Session acceptingSession;
    private FixEngineRunner leader;
    private TestSystem testSystem;

    @Before
    public void setUp()
    {
        initiatingMediaDriver = launchMediaDriver();

        acceptingCluster = new ArrayList<>();
        // Put them in the collection one by one, because if there's an error initializing
        // a latter runner, this ensures that the earlier ones get closed
        ids().forEach((ourId) -> acceptingCluster.add(new FixEngineRunner(ourId, ids())));

        // Start the cluster in a random order, ensuring that the node that is first in order
        // of library aeron channel order isn't started first.
        // This ensures that any failures in the Library's round-robin logic get tested regularly.
        final List<FixEngineRunner> randomOrderCluster = new ArrayList<>(acceptingCluster.subList(1, CLUSTER_SIZE));
        Collections.shuffle(randomOrderCluster);
        randomOrderCluster.forEach(FixEngineRunner::launch);
        acceptingCluster.get(0).launch();

        final LibraryConfiguration configuration = acceptingLibraryConfig(acceptingHandler);

        configuration.libraryAeronChannels(acceptingCluster
            .stream()
            .map(FixEngineRunner::libraryChannel)
            .collect(toList()));

        DebugLogger.log(LogTag.GATEWAY_CLUSTER_TEST, "Accepting Library id = %d%n", configuration.libraryId());

        this.leader = withTimeout(
            "Cluster failed to elect a leader",
            () -> findNewLeader(acceptingCluster),
            5000);

        acceptingLibrary = connect(configuration);

        assertNotNull("Unable to connect to any cluster members", acceptingLibrary);
        initiatingEngine = launchInitiatingEngine(libraryAeronPort);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(initiatingLibrary, acceptingLibrary);

        configuration
            .libraryAeronChannels()
            .forEach((channel) -> assertNotEquals(initiatingLibrary.currentAeronChannel(), channel));
    }

    @After
    public void tearDown()
    {
        closeLibrariesAndEngine();

        cleanupMediaDriver(initiatingMediaDriver);
    }

    @Test
    public void shouldExchangeMessagesInCluster()
    {
        connectFixSession();

        final long begin = System.nanoTime();
        roundTripOneMessage(initiatingSession, acceptingOtfAcceptor);
        final long position = roundTripOneMessage(acceptingSession, initiatingOtfAcceptor);

        assertEventuallyTrue(
            "Position never catches up to the required position",
            () ->
            {
                testSystem.poll();

                assertThat(acceptingHandler.sentPosition(), greaterThanOrEqualTo(position));
            });

        closeLibrariesAndEngine();
        final long end = System.nanoTime() + 1;

        assertAllClusterNodesHaveArchivedTestRequestMessage(begin, end, acceptingSession.id());

        assertAllClusterNodesHaveSameIndexFiles();
    }

    @Ignore
    @Test
    public void shouldExchangeMessagesAfterPartitionHeals()
    {
        connectFixSession();

        roundTripOneMessage(acceptingSession, initiatingOtfAcceptor);

        final FixEngineRunner oldLeader = leader;
        oldLeader.disable();
        logLeader(oldLeader, "Disabled old old leader (%s) [%s]%n");

        final List<FixEngineRunner> otherNodes = new ArrayList<>(acceptingCluster);
        otherNodes.remove(oldLeader);

        leader = withTimeout(
            "Cluster failed to elect a leader",
            () ->
            {
                testSystem.poll();
                return findNewLeader(otherNodes);
            },
            5000);

        assertNotEquals("Failed to change leader", oldLeader, leader);

        logLeader(leader, "Elected new leader: (%s) [%s]%n");

        final String msg = "Library Failed to connect, isConnected=" +
            acceptingLibrary.isConnected() +
            ", channel=" + acceptingLibrary.currentAeronChannel();

        assertEventuallyTrue(
            () -> msg,
            () ->
            {
                testSystem.poll();
                return connectedToLeader();
            },
            10_000,
            () -> {});

        DebugLogger.log(GATEWAY_CLUSTER_TEST, "Library has connected to new leader" + System.lineSeparator());

        initiatingSession.close();
        acceptingSession.close();
        acceptingHandler.clearSessions();
        initiatingHandler.clearSessions();

        assertEventuallyTrue(
            "Old library state not flushed out",
            () ->
            {
                testSystem.poll();

                assertOldSessionDisconnected(initiatingLibrary);
                assertOldSessionDisconnected(acceptingLibrary);
                assertOldSessionDisconnected(initiatingEngine);

                assertConnectedToLeader();
            });

        // TODO: re-enable this part of the test once it is stably passing
        //noinspection ConstantConditions,ConstantIfStatement
        if (false)
        {
            connectFixSession();

            DebugLogger.log(GATEWAY_CLUSTER_TEST, "Connected New Fix Session%n");

            roundTripOneMessage(acceptingSession, initiatingOtfAcceptor);

            DebugLogger.log(GATEWAY_CLUSTER_TEST, "Message Roundtrip%n");
        }
    }

    private Optional<FixEngineRunner> findNewLeader(final List<FixEngineRunner> nodes)
    {
        return nodes
            .stream()
            .filter(FixEngineRunner::isLeader)
            .findFirst();
    }

    private IntStream ids()
    {
        return IntStream.range(0, CLUSTER_SIZE);
    }

    private void assertConnectedToLeader()
    {
        final String leaderLibraryChannel = leader.libraryChannel();
        final String currentAcceptingAeronChannel = acceptingLibrary.currentAeronChannel();
        assertTrue("Accepting Library not connected", acceptingLibrary.isConnected());
        assertEquals("Library channel differs", leaderLibraryChannel, currentAcceptingAeronChannel);
    }

    private void assertOldSessionDisconnected(final FixEngine engine)
    {
        final List<LibraryInfo> libraries = SystemTestUtil.libraries(engine);
        libraries.forEach(
            (library) -> assertThat("Old session hasn't disconnected yet", library.sessions(), hasSize(0)));
    }

    private void assertOldSessionDisconnected(final FixLibrary library)
    {
        assertThat("Old session hasn't disconnected yet", library.sessions(), hasSize(0));
    }

    private void logLeader(final FixEngineRunner oldLeader, final String formatString)
    {
        DebugLogger.log(
            GATEWAY_CLUSTER_TEST,
            formatString,
            oldLeader.libraryChannel(),
            oldLeader.configuration().agentNamePrefix());
    }

    private boolean connectedToLeader()
    {
        final String libraryChannel = leader.libraryChannel();
        return acceptingLibrary.isConnected() && acceptingLibrary.currentAeronChannel().equals(libraryChannel);
    }

    private void connectFixSession()
    {
        final Builder builder = SessionConfiguration.builder();
        builder.address("localhost", leader.tcpPort());

        final SessionConfiguration config = builder
            .credentials("bob", "Uv1aegoh")
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .sequenceNumbersPersistent(true)
            .build();

        final Reply<Session> reply = initiatingLibrary.initiate(config);

        awaitLibraryReply(initiatingLibrary, acceptingLibrary, reply);
        assertEquals(Reply.State.COMPLETED, reply.state());
        initiatingSession = reply.resultIfPresent();

        assertConnected(initiatingSession);
        sessionLogsOn(testSystem, initiatingSession, 10_000);
        final long sessionId = acceptingHandler.awaitSessionIdFor(INITIATOR_ID, ACCEPTOR_ID,
            () ->
            {
                acceptingLibrary.poll(FRAGMENT_LIMIT);
                initiatingLibrary.poll(FRAGMENT_LIMIT);

                assertConnectedToLeader();
            },
            10_000);
        acceptingSession = acquireSession(acceptingHandler, acceptingLibrary, sessionId, testSystem);
        assertEquals(ACCEPTOR_ID, acceptingHandler.lastAcceptorCompId());
        assertEquals(INITIATOR_ID, acceptingHandler.lastInitiatorCompId());
    }

    private long roundTripOneMessage(final Session sendingSession, final FakeOtfAcceptor receivingHandler)
    {
        return assertTestRequestSentAndReceived(sendingSession, testSystem, receivingHandler);
    }

    private void assertAllClusterNodesHaveArchivedTestRequestMessage(
        final long begin, final long end, final long sessionId)
    {
        acceptingCluster.forEach(
            (runner) ->
            {
                final EngineConfiguration configuration = runner.configuration();
                final String logFileDir = configuration.logFileDir();
                final FixArchiveScanner scanner = new FixArchiveScanner(logFileDir);

                final MessageCounter messageCounter = new MessageCounter();

                final FixMessagePredicate predicate = messageTypeOf(TEST_REQUEST_MESSAGE)
                    .and(sessionOf(INITIATOR_ID, ACCEPTOR_ID))
                    .and(sessionOf(sessionId))
                    .and(between(begin, end));

                scanner.scan(
                    configuration.clusterAeronChannel(),
                    SENT,
                    filterBy(messageCounter, predicate),
                    Throwable::printStackTrace);

                if (messageCounter.messageCount() != 1)
                {
                    scanner.scan(
                        configuration.clusterAeronChannel(),
                        SENT,
                        (message, buffer, offset, length, header) -> System.out.println(message.body()),
                        Throwable::printStackTrace);
                }

                assertEquals(
                    configuration.nodeId() + " is missing the test request message from its log",
                    1,
                    messageCounter.messageCount());
            });
    }

    private void assertAllClusterNodesHaveSameIndexFiles()
    {
        final FixEngineRunner firstNode = acceptingCluster.get(0);
        final String logFileDir = firstNode.configuration().logFileDir();
        final Map<Long, Integer> firstReceivedSequenceNumberIndex = firstNode.readReceivedSequenceNumberIndex();
        final Map<Long, Integer> firstSentSequenceNumberIndex = firstNode.readSentSequenceNumberIndex();

        acceptingCluster.stream().skip(1).forEach(
            (runner) ->
            {
                final String otherLogFileDir = runner.configuration().logFileDir();

                assertFilesEqual(logFileDir, otherLogFileDir, DEFAULT_SESSION_ID_FILE);

                final Map<Long, Integer> receivedSequenceNumberIndex = runner.readReceivedSequenceNumberIndex();
                final Map<Long, Integer> sentSequenceNumberIndex = runner.readSentSequenceNumberIndex();

                assertEquals(
                    firstNode.nodeId() + " and " + runner.nodeId() + " disagree on received sequence numbers",
                    receivedSequenceNumberIndex,
                    firstReceivedSequenceNumberIndex);

                assertEquals(
                    firstNode.nodeId() + " and " + runner.nodeId() + " disagree on sent sequence numbers",
                    firstSentSequenceNumberIndex,
                    sentSequenceNumberIndex);
            });
    }

    private void assertFilesEqual(
        final String logFileDir, final String otherLogFileDir, final String path)
    {
        final File file = new File(logFileDir, path);
        assertTrue(file.getAbsolutePath(), file.exists());

        final File otherFile = new File(otherLogFileDir, path);
        assertTrue(otherFile.getAbsolutePath(), otherFile.exists());

        assertEquals("lengths differ", file.length(), otherFile.length());

        try
        {
            final byte[] bytes = Files.readAllBytes(file.toPath());
            final byte[] otherBytes = Files.readAllBytes(otherFile.toPath());

            assertArrayEquals("For file: " + path, bytes, otherBytes);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    class MessageCounter implements FixMessageConsumer
    {
        private int messageCount = 0;

        public void onMessage(
            final FixMessageDecoder fixMessage,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            messageCount++;
        }

        int messageCount()
        {
            return messageCount;
        }
    }

    // crash? engine process
    // initiator connect to wrong node in cluster
    // library connect to wrong node in cluster
    // partition TCP but not cluster,

    private void closeLibrariesAndEngine()
    {
        try
        {
            closeAll(acceptingLibrary, initiatingLibrary, initiatingEngine);
            closeAll(acceptingCluster);
        }
        finally
        {
            cleanupMediaDriver(initiatingMediaDriver);
        }
    }
}
