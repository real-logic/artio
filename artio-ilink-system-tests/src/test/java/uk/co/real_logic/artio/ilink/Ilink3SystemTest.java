/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.ilink;

import io.aeron.archive.ArchivingMediaDriver;
import org.agrona.CloseHelper;
import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.system_tests.TestSystem;

import java.io.IOException;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.ilink.ILink3SessionConfiguration.DEFAULT_REQUESTED_KEEP_ALIVE_INTERVAL;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class Ilink3SystemTest
{
    private static final String ACCESS_KEY_ID = "12345678901234567890";
    private static final String SESSION_ID = "ABC";
    private static final String FIRM_ID = "DEFGH";
    private static final String USER_KEY = "somethingprivate";

    private int port = unusedPort();
    private ArchivingMediaDriver mediaDriver;
    private TestSystem testSystem;
    private FixEngine engine;
    private FixLibrary library;
    private ILink3TestServer testServer;
    private Reply<ILink3Session> reply;
    private ILink3Session session;

    public void launch(final boolean printErrorMessages)
    {
        delete(CLIENT_LOGS);

        mediaDriver = launchMediaDriver();

        final EngineConfiguration engineConfig = new EngineConfiguration()
            .logFileDir(CLIENT_LOGS)
            .scheduler(new LowResourceEngineScheduler())
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .libraryAeronChannel(IPC_CHANNEL)
            .printErrorMessages(printErrorMessages);
        engine = FixEngine.launch(engineConfig);

        testSystem = new TestSystem();

        final LibraryConfiguration libraryConfig = new LibraryConfiguration()
            .libraryAeronChannels(singletonList(IPC_CHANNEL))
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS);
        library = testSystem.connect(libraryConfig);
    }

    @After
    public void close()
    {
        testSystem.awaitBlocking(() -> CloseHelper.close(engine));
        CloseHelper.close(library);
        cleanupMediaDriver(mediaDriver);
    }

    @Test
    public void shouldEstablishConnectionAtBeginningOfWeek() throws IOException
    {
        launch(true);

        final ILink3SessionConfiguration sessionConfiguration = sessionConfiguration();

        testServer = new ILink3TestServer(port, () -> reply = library.initiate(sessionConfiguration), testSystem);

        testServer.readNegotiate(ACCESS_KEY_ID, FIRM_ID);
        testServer.writeNegotiateResponse();

        testServer.readEstablish(ACCESS_KEY_ID, FIRM_ID, SESSION_ID, DEFAULT_REQUESTED_KEEP_ALIVE_INTERVAL);
        testServer.writeEstablishAck();

        testSystem.awaitCompletedReplies(reply);
        session = reply.resultIfPresent();
        assertNotNull(session);

        assertEquals(session.state(), ILink3Session.State.ESTABLISHED);
        assertEquals(testServer.uuid(), session.uuid());
    }

    @Test
    public void shouldTerminateConnection() throws IOException
    {
        shouldEstablishConnectionAtBeginningOfWeek();

        startTerminate();

        testServer.readTerminate();
        testServer.writeTerminate();

        testSystem.awaitUnbind(session);

        assertDisconnected();
    }

    @Test
    public void shouldExchangeInitiatedTerminateConnection() throws IOException
    {
        shouldEstablishConnectionAtBeginningOfWeek();

        testServer.writeTerminate();

        testSystem.awaitUnbind(session);

        testServer.readTerminate();

        assertDisconnected();
    }

    @Test
    public void shouldProvideErrorUponConnectionFailure()
    {
        launch(false);

        final ILink3SessionConfiguration sessionConfiguration = sessionConfiguration();

        reply = library.initiate(sessionConfiguration);
        testSystem.awaitReply(reply);
        assertEquals(Reply.State.ERRORED, reply.state());
        assertThat(reply.error().getMessage(), containsString("UNABLE_TO_CONNECT"));
    }

    private void startTerminate()
    {
        testSystem.awaitSend(
            "Failed to send terminate", () -> session.terminate("shutdown", 0));
    }

    private ILink3SessionConfiguration sessionConfiguration()
    {
        return new ILink3SessionConfiguration()
            .host("localhost")
            .port(port)
            .sessionId(SESSION_ID)
            .firmId(FIRM_ID)
            .userKey(USER_KEY)
            .accessKeyId(ACCESS_KEY_ID);
    }

    private void assertDisconnected()
    {
        testServer.assertDisconnected();
        assertThat(library.iLink3Sessions(), hasSize(0));
    }
}
