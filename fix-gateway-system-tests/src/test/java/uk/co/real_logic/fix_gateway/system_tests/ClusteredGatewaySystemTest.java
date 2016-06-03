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
import org.agrona.CloseHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.Reply;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration.Builder;
import uk.co.real_logic.fix_gateway.session.Session;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.agrona.CloseHelper.close;
import static org.junit.Assert.assertNotNull;
import static uk.co.real_logic.fix_gateway.TestFixtures.*;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

@Ignore
public class ClusteredGatewaySystemTest
{
    private static final int CLUSTER_SIZE = 3;
    private static final String CLUSTER_AERON_CHANNEL = clusteredAeronChannel();

    private List<Integer> tcpPorts;
    private List<Integer> libraryPorts;
    private int libraryAeronPort = unusedPort();

    private MediaDriver mediaDriver;

    private List<FixEngine> acceptingEngineCluster;
    private FixLibrary acceptingLibrary;

    private FixEngine initiatingEngine;
    private FixLibrary initiatingLibrary;

    private FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler acceptingHandler = new FakeHandler(acceptingOtfAcceptor);

    private FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler initiatingHandler = new FakeHandler(initiatingOtfAcceptor);

    private int leader;
    private Session initiatingSession;
    private Session acceptingSession;

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();
        tcpPorts = allocatePorts();
        libraryPorts = allocatePorts();

        // TODO: be able to disconnect TCP connections when failing the machine
        acceptingEngineCluster = IntStream
            .range(0, CLUSTER_SIZE)
            .mapToObj(i ->
            {
                final String acceptorLogs = ACCEPTOR_LOGS + i;
                delete(acceptorLogs);
                final EngineConfiguration configuration = new EngineConfiguration();

                setupAuthentication(ACCEPTOR_ID, INITIATOR_ID, configuration);

                configuration
                    .bindTo("localhost", tcpPorts.get(i))
                    .libraryAeronChannel(libraryChannel(i))
                    .monitoringFile(acceptorMonitoringFile("engineCounters" + i))
                    .logFileDir(acceptorLogs)
                    .clusterAeronChannel(CLUSTER_AERON_CHANNEL);

                return FixEngine.launch(configuration);
            })
            .collect(toList());

        for (leader = 0; leader < CLUSTER_SIZE; leader = (leader + 1) % CLUSTER_SIZE)
        {
            try
            {
                acceptingLibrary = FixLibrary.connect(
                    acceptingLibraryConfig(
                        acceptingHandler, ACCEPTOR_ID, INITIATOR_ID, "fix-acceptor", libraryChannel(leader)));

                break;
            }
            catch (final IllegalStateException e)
            {
                // Connection fails, try next member of the cluster
            }
        }

        assertNotNull("Unable to connect to any cluster members", acceptingLibrary);

        initiatingEngine = launchInitiatingGateway(libraryAeronPort);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, 1);
    }

    private String libraryChannel(final int id)
    {
        return "udp://localhost:" + libraryPorts.get(id);
    }

    private List<Integer> allocatePorts()
    {
        return IntStream
            .range(0, CLUSTER_SIZE)
            .mapToObj(i -> unusedPort())
            .collect(toList());
    }

    @Test
    public void shouldExchangeMessagesInCluster()
    {
        final Builder builder = SessionConfiguration.builder();
        tcpPorts.forEach(port -> builder.address("localhost", port));

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

        sendTestRequest(initiatingSession);

        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);

        // TODO: assert other nodes have stored the data.
    }

    @Test
    public void shouldExchangeMessagesAfterPartitionHeals()
    {

    }

    // crash? engine process
    // initiator connect to wrong node in cluster
    // library connect to wrong node in cluster
    // partition TCP but not cluster,

    @After
    public void tearDown()
    {
        close(acceptingLibrary);
        close(initiatingLibrary);

        close(initiatingEngine);
        if (acceptingEngineCluster != null)
        {
            acceptingEngineCluster.forEach(CloseHelper::close);
        }

        close(mediaDriver);
        cleanupDirectory(mediaDriver);
    }

}
