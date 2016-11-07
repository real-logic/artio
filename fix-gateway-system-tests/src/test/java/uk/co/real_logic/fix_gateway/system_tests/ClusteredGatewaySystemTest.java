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
package uk.co.real_logic.fix_gateway.system_tests;

import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.logger.FixArchiveScanner;
import uk.co.real_logic.fix_gateway.engine.logger.FixMessageConsumer;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration.Builder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;
import uk.co.real_logic.fix_gateway.session.Session;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.agrona.CloseHelper.close;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.LogTag.GATEWAY_CLUSTER;
import static uk.co.real_logic.fix_gateway.TestFixtures.*;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.decoder.Constants.TEST_REQUEST;
import static uk.co.real_logic.fix_gateway.dictionary.generation.Exceptions.closeAll;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.*;
import static uk.co.real_logic.fix_gateway.engine.logger.FixMessagePredicates.*;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class ClusteredGatewaySystemTest
{
    private static final int CLUSTER_SIZE = 3;
    private static final int TEST_TIMEOUT = 20_000;

    private int libraryAeronPort = unusedPort();
    private List<FixEngineRunner> cluster;

    private MediaDriver mediaDriver;
    private FixLibrary acceptingLibrary;

    private FixEngine initiatingEngine;
    private FixLibrary initiatingLibrary;

    private FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler acceptingHandler = new FakeHandler(acceptingOtfAcceptor);

    private FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler initiatingHandler = new FakeHandler(initiatingOtfAcceptor);

    private Session initiatingSession;
    private Session acceptingSession;
    private FixEngineRunner leader;

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();

        cluster = new ArrayList<>();
        // Put them in the collection one by one, because if there's an error initializing
        // a latter runner, this ensures that the earlier ones get closed
        ids().forEach((ourId) -> cluster.add(new FixEngineRunner(ourId, ids())));

        final LibraryConfiguration configuration = acceptingLibraryConfig(
            acceptingHandler, ACCEPTOR_ID, INITIATOR_ID, null)
            .replyTimeoutInMs(2_000);

        configuration.libraryAeronChannels(cluster
            .stream()
            .map(FixEngineRunner::libraryChannel)
            .collect(toList()));

        assertEventuallyTrue("Cluster failed to elect a leader",
            () ->
            {
                final Optional<FixEngineRunner> maybeLeader = findNewLeader();
                maybeLeader.ifPresent((leader) -> this.leader = leader);
                return maybeLeader.isPresent();
            });

        acceptingLibrary = FixLibrary.connect(configuration);

        assertNotNull("Unable to connect to any cluster members", acceptingLibrary);
        initiatingEngine = launchInitiatingGateway(libraryAeronPort);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
    }

    @After
    public void tearDown()
    {
        closeLibrariesAndEngine();

        close(mediaDriver);
        cleanupDirectory(mediaDriver);
    }

    private Optional<FixEngineRunner> findNewLeader()
    {
        return cluster
            .stream()
            .filter((leader) -> leader != this.leader)
            .filter(FixEngineRunner::isLeader)
            .findFirst();
    }

    private IntStream ids()
    {
        return IntStream.range(0, CLUSTER_SIZE);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldExchangeMessagesInCluster()
    {
        connectFixSession(1);

        final long begin = System.nanoTime();
        roundtripAMessage(initiatingSession, acceptingOtfAcceptor);
        final long position = roundtripAMessage(acceptingSession, initiatingOtfAcceptor);

        closeLibrariesAndEngine();
        final long end = System.nanoTime() + 1;

        assertThat(acceptingHandler.sentPosition(), greaterThanOrEqualTo(position));

        allClusterNodesHaveArchivedTestRequestMessage(begin, end, acceptingSession.id());

        allClusterNodesHaveSameIndexFiles();
    }

    @Ignore
    @Test(timeout = TEST_TIMEOUT)
    public void shouldExchangeMessagesAfterPartitionHeals()
    {
        connectFixSession(1);

        roundtripAMessage(acceptingSession, initiatingOtfAcceptor);

        final FixEngineRunner oldLeader = leader;
        oldLeader.disable();
        DebugLogger.log(GATEWAY_CLUSTER, "Disabled old old leader (%s)\n", oldLeader.libraryChannel());

        while (true)
        {
            initiatingLibrary.poll(1);
            acceptingLibrary.poll(1);

            final Optional<FixEngineRunner> leader = findNewLeader();
            if (leader.isPresent())
            {
                this.leader = leader.get();
                break;
            }

            ADMIN_IDLE_STRATEGY.idle();
        }

        ADMIN_IDLE_STRATEGY.reset();

        DebugLogger.log(GATEWAY_CLUSTER, "Elected new leader: (%s)\n", leader.libraryChannel());

        final String libraryChannel = leader.libraryChannel();
        while (notConnectedTo(libraryChannel))
        {
            initiatingLibrary.poll(1);
            acceptingLibrary.poll(1);

            ADMIN_IDLE_STRATEGY.idle();
        }

        ADMIN_IDLE_STRATEGY.reset();

        DebugLogger.log(GATEWAY_CLUSTER, "Library has connected to new leader\n");

        // TODO: acceptingLibrary disconnect/timeout
        // TODO: oldLeader.enable();

        initiatingSession.close();
        acceptingSession.close();
        acceptingHandler.clearSessions();
        initiatingHandler.clearSessions();

        connectFixSession(2);

        DebugLogger.log(GATEWAY_CLUSTER, "Connected New Fix Session\n");

        roundtripAMessage(acceptingSession, initiatingOtfAcceptor);

        DebugLogger.log(GATEWAY_CLUSTER, "Message Roundtrip\n");
    }

    private boolean notConnectedTo(final String libraryChannel)
    {
        return !acceptingLibrary.isConnected()
            || !acceptingLibrary.currentAeronChannel().equals(libraryChannel);
    }

    private void connectFixSession(final int initialSequenceNumber)
    {
        final Builder builder = SessionConfiguration.builder();
        builder.address("localhost", leader.tcpPort());

        final SessionConfiguration config = builder
            .credentials("bob", "Uv1aegoh")
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .initialSequenceNumber(initialSequenceNumber)
            .build();

        final Reply<Session> reply = initiatingLibrary.initiate(config);

        awaitReply(initiatingLibrary, reply);

        initiatingSession = reply.resultIfPresent();

        assertConnected(initiatingSession);
        sessionLogsOn(initiatingLibrary, acceptingLibrary, initiatingSession);
        final long sessionId = acceptingHandler.awaitSessionIdFor(INITIATOR_ID, ACCEPTOR_ID,
            () ->
            {
                acceptingLibrary.poll(1);
                initiatingLibrary.poll(1);
            });
        acceptingSession = acquireSession(acceptingHandler, acceptingLibrary, sessionId);
        assertEquals(ACCEPTOR_ID, acceptingHandler.lastAcceptorCompId());
        assertEquals(INITIATOR_ID, acceptingHandler.lastInitiatorCompId());
    }

    private long roundtripAMessage(final Session sendingSession, final FakeOtfAcceptor receivingHandler)
    {
        final long position = sendTestRequest(sendingSession);

        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, receivingHandler);

        return position;
    }

    private void allClusterNodesHaveArchivedTestRequestMessage(
        final long begin, final long end, final long sessionId)
    {
        cluster.forEach(
            (runner) ->
            {
                final EngineConfiguration configuration = runner.configuration();
                final String logFileDir = configuration.logFileDir();
                final FixArchiveScanner scanner = new FixArchiveScanner(logFileDir);
                final StreamIdentifier id = new StreamIdentifier(
                    configuration.clusterAeronChannel(), OUTBOUND_LIBRARY_STREAM);

                final TestRequestFinder testRequestFinder = new TestRequestFinder();
                scanner.forEachMessage(
                    id,
                    filterBy(
                        testRequestFinder,
                        messageTypeOf(TEST_REQUEST)
                            .and(sessionOf(INITIATOR_ID, ACCEPTOR_ID))
                            .and(sessionOf(sessionId))
                            .and(between(begin, end))),
                    Throwable::printStackTrace);

                if (!testRequestFinder.isPresent)
                {
                    scanner.forEachMessage(
                        id,
                        (message, buffer, offset, length, header) -> System.out.println(message.body()),
                        Throwable::printStackTrace);
                }

                assertTrue(configuration.nodeId() + " is missing the test request message from its log",
                    testRequestFinder.isPresent);
            });
    }

    private void allClusterNodesHaveSameIndexFiles()
    {
        final FixEngineRunner firstNode = cluster.get(0);
        final String logFileDir = firstNode.configuration().logFileDir();
        cluster.stream().skip(1).forEach(
            (runner) ->
            {
                final String otherLogFileDir = runner.configuration().logFileDir();

                assertFilesEqual(logFileDir, otherLogFileDir, DEFAULT_SESSION_ID_FILE);
                assertFilesEqual(logFileDir, otherLogFileDir, DEFAULT_SEQUENCE_NUMBERS_RECEIVED_FILE);
                assertFilesEqual(logFileDir, otherLogFileDir, DEFAULT_SEQUENCE_NUMBERS_SENT_FILE);
            });
    }

    private void assertFilesEqual(
        final String logFileDir, final String otherLogFileDir, final String path)
    {
        final File file = new File(logFileDir, path);
        assertTrue(file.getAbsolutePath(), file.exists());

        final File otherFile = new File(otherLogFileDir, path);
        assertTrue(otherFile.getAbsolutePath(), otherFile.exists());

        final long length = file.length();
        assertEquals("lengths differ", length, otherFile.length());

        try
        {
            final byte[] bytes = Files.readAllBytes(file.toPath());
            final byte[] otherBytes = Files.readAllBytes(otherFile.toPath());

            assertArrayEquals(bytes, otherBytes);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private class TestRequestFinder implements FixMessageConsumer
    {
        private boolean isPresent = false;

        public void onMessage(
            final FixMessageDecoder fixMessage,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            isPresent = true;
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
            closeAll(cluster);
        }
        finally
        {
            mediaDriver.close();
            cleanupDirectory(mediaDriver);
        }
    }
}
