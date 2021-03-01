/*
 * Copyright 2021 Monotonic Ltd.
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

import b3.entrypoint.fixp.sbe.EstablishAckDecoder;
import b3.entrypoint.fixp.sbe.NegotiateResponseDecoder;
import io.aeron.archive.ArchivingMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointIdentification;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.ILink3RetransmitHandler;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.messages.SessionReplyStatus;

import java.io.IOException;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.CLIENT_LOGS;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.TEST_REPLY_TIMEOUT_IN_MS;

public class BinaryEntrypointSystemTest
{

    private final int port = unusedPort();

    private ArchivingMediaDriver mediaDriver;
    private TestSystem testSystem;
    private FixEngine engine;
    private FixLibrary library;

    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final ILink3RetransmitHandler retransmitHandler = mock(ILink3RetransmitHandler.class);
    private final FakeFixPConnectionExistsHandler connectionExistsHandler = new FakeFixPConnectionExistsHandler();
    private final FakeBinaryEntrypointConnectionHandler connectionHandler = new FakeBinaryEntrypointConnectionHandler(
        notAppliedResponse ->
        {
        });
    private final FakeFixPConnectionAcquiredHandler connectionAcquiredHandler = new FakeFixPConnectionAcquiredHandler(
        connectionHandler);

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();

        testSystem = new TestSystem();

        final EngineConfiguration engineConfig = new EngineConfiguration()
            .logFileDir(CLIENT_LOGS)
            .scheduler(new LowResourceEngineScheduler())
            .libraryAeronChannel(IPC_CHANNEL)
//            .errorHandlerFactory(errorBuffer -> errorHandler)
//            .monitoringAgentFactory(MonitoringAgentFactory.none())
            .binaryFixPRetransmitHandler(retransmitHandler)
            .acceptBinaryEntryPoint()
            .bindTo("localhost", port)
            .deleteLogFileDirOnStart(true);

        engine = FixEngine.launch(engineConfig);

        final LibraryConfiguration libraryConfig = new LibraryConfiguration()
            .libraryAeronChannels(singletonList(IPC_CHANNEL))
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .fixPConnectionExistsHandler(connectionExistsHandler)
            .fixPConnectionAcquiredHandler(connectionAcquiredHandler);

//        libraryConfig
//            .errorHandlerFactory(errorBuffer -> errorHandler)
//            .monitoringAgentFactory(MonitoringAgentFactory.none());
        library = testSystem.connect(libraryConfig);
    }

    @Test
    public void shouldAcceptLogonFromClient() throws IOException
    {
        try (BinaryEntrypointClient client = new BinaryEntrypointClient(port, testSystem))
        {
            client.writeNegotiate();

            testSystem.await("connection doesn't exist", connectionExistsHandler::invoked);
            assertEquals(BinaryEntrypointClient.SESSION_ID, connectionExistsHandler.lastSurrogateSessionId());
            final BinaryEntryPointIdentification id =
                (BinaryEntryPointIdentification)connectionExistsHandler.lastIdentification();
            assertEquals(BinaryEntrypointClient.SESSION_ID, id.sessionID());
            assertEquals(1, id.sessionVerID());
            final Reply<SessionReplyStatus> reply = connectionExistsHandler.lastReply();

            testSystem.awaitCompletedReply(reply);
            assertEquals(SessionReplyStatus.OK, reply.resultIfPresent());

            testSystem.await("connection not acquired", connectionAcquiredHandler::invoked);

            final NegotiateResponseDecoder response = client.readNegotiateResponse();
            assertEquals(BinaryEntrypointClient.SESSION_ID, response.sessionID());
            assertEquals(1, response.sessionVerID());
            assertEquals(BinaryEntrypointClient.FIRM_ID, response.enteringFirm());

            client.writeEstablish();
            final EstablishAckDecoder establishAck = client.readEstablishAck();
            assertEquals(BinaryEntrypointClient.SESSION_ID, establishAck.sessionID());
            assertEquals(1, establishAck.sessionVerID());
            assertEquals(1, establishAck.nextSeqNo());
            assertEquals(0, establishAck.lastIncomingSeqNo());
        }
    }

    // TODO: exchanging business messages
    // TODO: terminate
    // TODO: timeout disconnect
    // TODO: heartbeat / timeout

    @After
    public void close()
    {
        closeArtio();
        cleanupMediaDriver(mediaDriver);

        verifyNoInteractions(errorHandler);

        Mockito.framework().clearInlineMocks();
    }

    private void closeArtio()
    {
        testSystem.awaitBlocking(() -> CloseHelper.close(engine));
        CloseHelper.close(library);
    }
}
