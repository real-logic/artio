/*
 * Copyright 2015-2022 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.system_tests;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.session.Session;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ConnectAfterTimeoutSystemTest extends AbstractGatewayToGatewaySystemTest
{
    private DebugTcpChannelSupplier debugTcpChannelSupplier;

    @Before
    public void launch()
    {
        delete(ACCEPTOR_LOGS);

        mediaDriver = launchMediaDriver();

        delete(CLIENT_LOGS);
        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort, nanoClock);
        initiatingConfig.channelSupplierFactory(config ->
        {
            debugTcpChannelSupplier = new DebugTcpChannelSupplier(config);
            return debugTcpChannelSupplier;
        });
        initiatingEngine = FixEngine.launch(initiatingConfig);

        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, nanoClock);
        testSystem = new TestSystem(initiatingLibrary);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void connectsOnceSystemIsUp()
    {
        debugTcpChannelSupplier.disable();

        // First reply times out
        final Reply<Session> firstConnectReply = completeInitiateSession();
        assertEquals(Reply.State.TIMED_OUT, firstConnectReply.state());

        // Second reply also times out
        final Reply<Session> secondConnectReply = completeInitiateSession();
        assertEquals(Reply.State.TIMED_OUT, secondConnectReply.state());

        debugTcpChannelSupplier.enable();

        // Launch the acceptor
        launchAcceptingEngine();
        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler, nanoClock);
        acceptingLibrary = testSystem.connect(acceptingLibraryConfig);

        // Now it should connect
        connectSessions();
        messagesCanBeExchanged();
        assertInitiatingSequenceIndexIs(0);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void testConnectingAfterConnectionTimeouts()
    {
        // Launch the acceptor
        launchAcceptingEngine();
        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler, nanoClock);
        acceptingLibrary = testSystem.connect(acceptingLibraryConfig);

        // Make connections time out
        debugTcpChannelSupplier.pauseConnects();

        // First reply times out
        final Reply<Session> firstConnectReply = completeInitiateSession();
        assertEquals(Reply.State.TIMED_OUT, firstConnectReply.state());

        // Second reply also times out
        final Reply<Session> secondConnectReply = completeInitiateSession();
        assertEquals(Reply.State.TIMED_OUT, secondConnectReply.state());

        // Make connections work again
        debugTcpChannelSupplier.unpauseConnects();

        // Now it should connect
        connectSessions();
        messagesCanBeExchanged();
        assertInitiatingSequenceIndexIs(0);
    }

    private Reply<Session> completeInitiateSession()
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials(USERNAME, PASSWORD)
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .timeoutInMs(200)
            .build();

        final Reply<Session> firstConnectReply = initiatingLibrary.initiate(config);
        testSystem.awaitReply(firstConnectReply);
        return firstConnectReply;
    }
}
