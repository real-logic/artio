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
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.logger.FixArchiveScanner;
import uk.co.real_logic.fix_gateway.engine.logger.FixMessageConsumer;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.Reply;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration.Builder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;
import uk.co.real_logic.fix_gateway.session.Session;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.agrona.CloseHelper.close;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.TestFixtures.*;
import static uk.co.real_logic.fix_gateway.decoder.Constants.TEST_REQUEST;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.*;
import static uk.co.real_logic.fix_gateway.engine.logger.FixMessagePredicates.*;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class ClusteredGatewaySystemTest
{
    private static final int CLUSTER_SIZE = 3;

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

        cluster = ids()
            .mapToObj(ourId -> new FixEngineRunner(ourId, ids()))
            .collect(toList());

        final LibraryConfiguration configuration = acceptingLibraryConfig(
            acceptingHandler, ACCEPTOR_ID, INITIATOR_ID, "fix-acceptor", null)
            .replyTimeoutInMs(20_000);

        configuration.libraryAeronChannels(
            cluster
                .stream()
                .map(FixEngineRunner::libraryChannel)
                .collect(toList()));

        acceptingLibrary = FixLibrary.connect(configuration);

        leader = cluster
            .stream()
            .filter(runner -> runner.libraryChannel().equals(acceptingLibrary.currentAeronChannel()))
            .findFirst()
            .get();

        assertNotNull("Unable to connect to any cluster members", acceptingLibrary);
        initiatingEngine = launchInitiatingGateway(libraryAeronPort);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, 1);
    }

    private IntStream ids()
    {
        return IntStream.range(0, CLUSTER_SIZE);
    }

    @Test
    public void shouldExchangeMessagesInCluster()
    {
        connectFixSession();

        final long begin = System.nanoTime();
        roundtripAMessage();

        closeLibrariesAndEngine();
        final long end = System.nanoTime() + 1;

        allClusterNodesHaveArchivedTestRequestMessage(begin, end, acceptingSession.id());

        allClusterNodesHaveSameIndexFiles();
    }

    private void connectFixSession()
    {
        final Builder builder = SessionConfiguration.builder();
        builder.address("localhost", leader.tcpPort());

        final SessionConfiguration config = builder
            .credentials("bob", "Uv1aegoh")
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .build();

        final Reply<Session> reply = initiatingLibrary.initiate(config);

        awaitReply(initiatingLibrary, reply);

        initiatingSession = reply.resultIfPresent();

        assertConnected(initiatingSession);
        sessionLogsOn(initiatingLibrary, acceptingLibrary, initiatingSession);
        acceptingSession = acquireSession(acceptingHandler, acceptingLibrary);
        assertEquals(ACCEPTOR_ID, acceptingHandler.lastAcceptorCompId());
        assertEquals(INITIATOR_ID, acceptingHandler.lastInitiatorCompId());
    }

    private void roundtripAMessage()
    {
        sendTestRequest(initiatingSession);

        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);
    }

    private void allClusterNodesHaveArchivedTestRequestMessage(
        final long begin, final long end, final long sessionId)
    {
        cluster.forEach(runner ->
        {
            final EngineConfiguration configuration = runner.configuration();
            final String logFileDir = configuration.logFileDir();
            final FixArchiveScanner scanner = new FixArchiveScanner(logFileDir);
            final StreamIdentifier id = new StreamIdentifier(
                configuration.clusterAeronChannel(), OUTBOUND_LIBRARY_STREAM);

            final TestRequestFinder testRequestFinder = new TestRequestFinder();
            scanner.forEachMessage(
                id,
                filterBy(testRequestFinder,
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
        cluster.stream().skip(1).forEach(runner ->
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
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    @Ignore
    @Test
    public void shouldExchangeMessagesAfterPartitionHeals()
    {
        connectFixSession();

        roundtripAMessage();
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

    @After
    public void tearDown()
    {
        closeLibrariesAndEngine();

        close(mediaDriver);
        cleanupDirectory(mediaDriver);
    }

    private void closeLibrariesAndEngine()
    {
        close(acceptingLibrary);
        close(initiatingLibrary);

        close(initiatingEngine);
        cluster.forEach(FixEngineRunner::close);
    }

}
