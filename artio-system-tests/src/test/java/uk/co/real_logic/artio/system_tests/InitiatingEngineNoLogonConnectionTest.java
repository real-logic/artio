/*
 * Copyright 2015-2023 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.session.Session;

import java.io.IOException;
import java.net.ServerSocket;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.Reply.State.ERRORED;
import static uk.co.real_logic.artio.Reply.State.TIMED_OUT;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class InitiatingEngineNoLogonConnectionTest extends AbstractGatewayToGatewaySystemTest
{
    private final ServerSocket serverSocket;
    private final Thread serverThread;

    public InitiatingEngineNoLogonConnectionTest() throws IOException
    {
        serverSocket = new ServerSocket(port);
        serverThread = new Thread(
            () ->
            {
                try
                {
                    while (true)
                    {
                        serverSocket.accept();
                    }
                }
                catch (final IOException ignore)
                {
                    // Deliberately blank as closing the socket will cause an exception to be thrown.
                }
            });
    }

    @Before
    public void launch()
    {
        deleteLogs();
        mediaDriver = launchMediaDriver();

        initiatingEngine = FixEngine.launch(initiatingConfig(libraryAeronPort, nanoClock));
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, nanoClock);
        testSystem = new TestSystem(initiatingLibrary);

        serverThread.setDaemon(true);
        serverThread.start();
    }

    @After
    public void stopServerThread() throws Exception
    {
        serverSocket.close();
        if (serverThread.isAlive())
        {
            serverThread.join();
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldTimeoutWhenConnectingToUnresponsiveEngine()
    {
        final Reply<Session> secondConnectReply = initiateSession();
        assertEquals(Reply.State.EXECUTING, secondConnectReply.state());
        testSystem.awaitReply(secondConnectReply);
        assertThat(secondConnectReply.state(), anyOf(is(TIMED_OUT), is(ERRORED)));
    }

    private Reply<Session> initiateSession()
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials(USERNAME, PASSWORD)
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .timeoutInMs(200)
            .build();

        return initiatingLibrary.initiate(config);
    }
}
