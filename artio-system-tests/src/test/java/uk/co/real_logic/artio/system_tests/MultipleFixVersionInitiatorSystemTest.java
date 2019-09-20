/*
 * Copyright 2019 Monotonic Ltd.
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

import org.agrona.CloseHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.builder.AbstractLogonEncoder;
import uk.co.real_logic.artio.builder.AbstractLogoutEncoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.fixt.ApplVerID;
import uk.co.real_logic.artio.fixt.FixDictionaryImpl;
import uk.co.real_logic.artio.fixt.builder.LogonEncoder;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.session.SessionCustomisationStrategy;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.unusedPort;
import static uk.co.real_logic.artio.fixt.ApplVerID.FIX50;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class MultipleFixVersionInitiatorSystemTest extends AbstractGatewayToGatewaySystemTest
{
    private static final String FIXT_ACCEPTOR_ID = "fixt-acceptor";
    private static final int FIXT_INBOUND_LIBRARY_STREAM = 11;
    private static final int FIXT_OUTBOUND_LIBRARY_STREAM = 12;
    private static final int FIXT_OUTBOUND_REPLAY_STREAM = 13;
    private static final int FIXT_ARCHIVE_REPLAY_STREAM = 14;

    private int fixtPort = unusedPort();

    private FixEngine fixtAcceptingEngine;
    private FixLibrary fixtAcceptingLibrary;

    private FakeOtfAcceptor fixtAcceptingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler fixtAcceptingHandler = new FakeHandler(fixtAcceptingOtfAcceptor);

    private Session fixtAcceptingSession;
    private Session fixtInitiatingSession;

    @Before
    public void launch()
    {
        delete(ACCEPTOR_LOGS);

        mediaDriver = launchMediaDriver();

        launchAcceptingEngine();
        launchFixTAcceptingEngine();
        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        acceptingLibrary = connect(acceptingLibraryConfig(acceptingHandler));
        connectFixTAcceptingLibrary();

        final LibraryConfiguration configuration = initiatingLibraryConfig(libraryAeronPort, initiatingHandler);
        configuration.sessionCustomisationStrategy(new FixTSessionCustomisationStrategy(FIX50));
        initiatingLibrary = connect(configuration);

        testSystem = new TestSystem(acceptingLibrary, fixtAcceptingLibrary, initiatingLibrary);
    }

    @After
    public void closeFixT()
    {
        CloseHelper.close(fixtAcceptingLibrary);
        CloseHelper.close(fixtAcceptingEngine);
    }

    private void connectFixTAcceptingLibrary()
    {
        final LibraryConfiguration configuration = new LibraryConfiguration();
        setupCommonConfig(FIXT_ACCEPTOR_ID, INITIATOR_ID, configuration);

        configuration
            .sessionExistsHandler(fixtAcceptingHandler)
            .sessionAcquireHandler(fixtAcceptingHandler)
            .sentPositionHandler(fixtAcceptingHandler)
            .libraryAeronChannels(singletonList(IPC_CHANNEL))
            .libraryName("fixtAccepting")
            .inboundLibraryStream(FIXT_INBOUND_LIBRARY_STREAM)
            .outboundLibraryStream(FIXT_OUTBOUND_LIBRARY_STREAM)
            .sessionCustomisationStrategy(new FixTSessionCustomisationStrategy(FIX50));

        fixtAcceptingLibrary = connect(configuration);
    }

    private void launchFixTAcceptingEngine()
    {
        final EngineConfiguration configuration = new EngineConfiguration();
        final MessageValidationStrategy validationStrategy = setupCommonConfig(
            FIXT_ACCEPTOR_ID,
            INITIATOR_ID, configuration);
        final AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.of(validationStrategy);
        configuration.authenticationStrategy(authenticationStrategy);

        final EngineConfiguration fixtAcceptingConfiguration = configuration
            .bindTo("localhost", fixtPort)
            .libraryAeronChannel(IPC_CHANNEL)
            .monitoringFile(acceptorMonitoringFile("fixtEngineCounters"))
            .inboundLibraryStream(FIXT_INBOUND_LIBRARY_STREAM)
            .outboundLibraryStream(FIXT_OUTBOUND_LIBRARY_STREAM)
            .outboundReplayStream(FIXT_OUTBOUND_REPLAY_STREAM)
            .archiveReplayStream(FIXT_ARCHIVE_REPLAY_STREAM)
            .logFileDir(ACCEPTOR_LOGS)
            .scheduler(new LowResourceEngineScheduler());

        fixtAcceptingConfiguration
            .acceptorfixDictionary(FixDictionaryImpl.class)
            .sessionCustomisationStrategy(new FixTSessionCustomisationStrategy(FIX50));

        fixtAcceptingEngine = FixEngine.launch(fixtAcceptingConfiguration);
    }

    @Test
    public void messagesCanBeSentFromInitiatorToBothAcceptors()
    {
        connectSessions();
        connectFixTSessions();

        bothSessionsCanExchangeMessages();
    }

    @Test
    public void sessionsCanBeAcquired()
    {
        connectSessions();
        acquireAcceptingSession();

        connectFixTSessions();
        acquireFixTSession();

        bothSessionsCanExchangeMessages();
    }

    private void bothSessionsCanExchangeMessages()
    {
        messagesCanBeExchanged();

        final FixDictionaryImpl fixtDictionary = new FixDictionaryImpl();

        final String testReqID = testReqId();
        sendTestRequest(fixtInitiatingSession, testReqID, fixtDictionary);
        assertReceivedSingleHeartbeat(testSystem, initiatingOtfAcceptor, testReqID);
    }

    private void acquireFixTSession()
    {
        final long sessionId = fixtAcceptingHandler.awaitSessionId(testSystem::poll);

        fixtAcceptingSession = acquireSession(fixtAcceptingHandler, fixtAcceptingLibrary, sessionId, testSystem);
        assertEquals(INITIATOR_ID, fixtAcceptingHandler.lastInitiatorCompId());
        assertEquals(FIXT_ACCEPTOR_ID, fixtAcceptingHandler.lastAcceptorCompId());
        assertNotNull("unable to acquire accepting session", fixtAcceptingSession);
    }

    private void connectFixTSessions()
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", fixtPort)
            .credentials(USERNAME, PASSWORD)
            .senderCompId(INITIATOR_ID)
            .targetCompId(FIXT_ACCEPTOR_ID)
            .fixDictionary(FixDictionaryImpl.class)
            .build();

        final Reply<Session> reply = initiatingLibrary.initiate(config);
        fixtInitiatingSession = completeConnectSessions(reply);
    }
}

class FixTSessionCustomisationStrategy implements SessionCustomisationStrategy
{
    private final ApplVerID applVerID;

    FixTSessionCustomisationStrategy(final ApplVerID applVerID)
    {
        this.applVerID = applVerID;
    }

    public void configureLogon(final AbstractLogonEncoder abstractLogon, final long sessionId)
    {
        if (abstractLogon instanceof LogonEncoder)
        {
            final LogonEncoder logon = (LogonEncoder)abstractLogon;
            logon.defaultApplVerID(applVerID.representation());
        }
    }

    public void configureLogout(final AbstractLogoutEncoder logout, final long sessionId)
    {
    }
}
