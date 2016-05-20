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
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

@Ignore
public class ClusteredGatewaySystemTest
{
    private static final int CLUSTER_SIZE = 3;

    private List<Integer> ports;
    private int initAeronPort = unusedPort();

    private MediaDriver mediaDriver;

    private List<FixEngine> acceptingEngineCluster;
    private FixLibrary acceptingLibrary;

    private FixEngine initiatingEngine;
    private FixLibrary initiatingLibrary;

    private FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler acceptingHandler = new FakeHandler(acceptingOtfAcceptor);

    private FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler initiatingHandler = new FakeHandler(initiatingOtfAcceptor);

    private Session initiatingSession;
    private Session acceptingSession;

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();
        ports = IntStream
            .range(0, CLUSTER_SIZE)
            .mapToObj(i -> unusedPort())
            .collect(toList());

        // TODO: disconnect TCP connections when failing the machine
        acceptingEngineCluster = IntStream
            .range(0, CLUSTER_SIZE)
            .mapToObj(i ->
            {
                final String acceptorLogs = ACCEPTOR_LOGS + i;
                delete(acceptorLogs);
                return FixEngine.launch(acceptingConfig(
                    ports.get(i), "engineCounters" + i, ACCEPTOR_ID, INITIATOR_ID, acceptorLogs));
            })
            .collect(toList());

        initiatingEngine = launchInitiatingGateway(initAeronPort);
        acceptingLibrary = newAcceptingLibrary(acceptingHandler);
        initiatingLibrary = newInitiatingLibrary(initAeronPort, initiatingHandler, 1);
    }

    @Test
    public void shouldExchangeMessagesInCluster()
    {
        final Builder builder = SessionConfiguration.builder();
        ports.forEach(port -> builder.address("localhost", port));

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
        acceptingEngineCluster.forEach(CloseHelper::close);

        close(mediaDriver);
    }

}
