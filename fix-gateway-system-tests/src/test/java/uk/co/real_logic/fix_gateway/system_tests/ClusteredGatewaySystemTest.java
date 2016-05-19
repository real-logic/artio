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

    private int port = unusedPort();
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

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();
        acceptingEngineCluster = IntStream
            .range(0, CLUSTER_SIZE)
            .mapToObj(i -> {
                final String acceptorLogs = ACCEPTOR_LOGS + i;
                delete(acceptorLogs);
                return FixEngine.launch(acceptingConfig(
                    port, "engineCounters" + i, ACCEPTOR_ID, INITIATOR_ID, acceptorLogs));
            })
            .collect(toList());

        initiatingEngine = launchInitiatingGateway(initAeronPort);
        acceptingLibrary = newAcceptingLibrary(acceptingHandler);
        initiatingLibrary = newInitiatingLibrary(initAeronPort, initiatingHandler, 1);
    }

    @After
    public void tearDown()
    {
        close(acceptingLibrary);
        close(initiatingLibrary);

        close(initiatingEngine);
        acceptingEngineCluster.forEach(CloseHelper::close);

        close(mediaDriver);
    }

    @Test
    public void shouldExchangeMessagesInCluster()
    {

    }

    @Test
    public void shouldExchangeMessagesAfterPartion()
    {

    }

}
