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

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.builder.AbstractLogonEncoder;
import uk.co.real_logic.artio.builder.AbstractLogoutEncoder;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.fixt.ApplVerID;
import uk.co.real_logic.artio.fixt.FixDictionaryImpl;
import uk.co.real_logic.artio.fixt.builder.HeaderEncoder;
import uk.co.real_logic.artio.fixt.builder.LogonEncoder;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.other.Constants;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.session.SessionCustomisationStrategy;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.fixt.ApplVerID.FIX50;
import static uk.co.real_logic.artio.fixt.Constants.APPL_VER_ID;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class MultipleFixVersionSystemTest extends AbstractGatewayToGatewaySystemTest
{
    private static final String FIXT_ACCEPTOR_LOGS = "fixt-acceptor-logs";
    private static final String FIXT_ACCEPTOR_ID = "fixt-acceptor";

    private static final String OTHER_INITIATOR_ID = "otherInitiator";
    private static final Class<? extends FixDictionary> OTHER_FIX_DICTIONARY =
        uk.co.real_logic.artio.other.FixDictionaryImpl.class;
    static final String TEST_VALUE = "test";

    private Session fixtInitiatingSession;
    private Session fixtAcceptingSession;

    @Before
    public void launch()
    {
        deleteLogs();
        delete(FIXT_ACCEPTOR_LOGS);

        mediaDriver = launchMediaDriver();

        launchMultiVersionAcceptingEngine();
        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        connectMultiVersionAcceptingLibrary();

        final LibraryConfiguration configuration = initiatingLibraryConfig(libraryAeronPort, initiatingHandler);
        configuration.sessionCustomisationStrategy(new FixTSessionCustomisationStrategy(FIX50));
        initiatingLibrary = connect(configuration);

        testSystem = new TestSystem(acceptingLibrary, acceptingLibrary, initiatingLibrary);
    }

    private void connectMultiVersionAcceptingLibrary()
    {
        final LibraryConfiguration configuration = new LibraryConfiguration();

        configuration
            .sessionExistsHandler(acceptingHandler)
            .sessionAcquireHandler(acceptingHandler)
            .libraryAeronChannels(singletonList(IPC_CHANNEL))
            .libraryName("accepting")
            .sessionCustomisationStrategy(new FixTSessionCustomisationStrategy(FIX50));

        acceptingLibrary = connect(configuration);
    }

    private void launchMultiVersionAcceptingEngine()
    {
        final EngineConfiguration configuration = new EngineConfiguration();
        final EngineConfiguration acceptingConfiguration = configuration
            .bindTo("localhost", port)
            .libraryAeronChannel(IPC_CHANNEL)
            .monitoringFile(acceptorMonitoringFile("engineCounters"))
            .logFileDir(FIXT_ACCEPTOR_LOGS)
            .scheduler(new LowResourceEngineScheduler())
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .authenticationStrategy(new MultiVersionAuthenticationStrategy(OTHER_INITIATOR_ID, OTHER_FIX_DICTIONARY));

        acceptingConfiguration
            .overrideAcceptorFixDictionary(FixDictionaryImpl.class)
            .sessionCustomisationStrategy(new FixTSessionCustomisationStrategy(FIX50));

        acceptingEngine = FixEngine.launch(acceptingConfiguration);
    }

    @Test
    public void shouldBeAbleToSendMessagesFromInitiatorToBothAcceptors()
    {
        connectSessions();
        connectFixTSessions();

        bothSessionsCanExchangeMessages();
    }

    @Test
    public void shouldBeAbleToAquireSessions()
    {
        connectSessions();
        acquireAcceptingSession();

        acceptingHandler.clearSessionExistsInfos();

        connectFixTSessions();
        acquireFixTSession();

        bothSessionsCanExchangeMessages();

        assertEquals(2, acceptingSession.lastReceivedMsgSeqNum());
        assertEquals(2, fixtAcceptingSession.lastReceivedMsgSeqNum());
        assertEquals("FIX.4.4", acceptingSession.beginString());
        assertEquals("FIXT.1.1", fixtAcceptingSession.beginString());

        assertHeaderHasApplVerId(acceptingOtfAcceptor);
    }

    @Test
    public void shouldBeAbleToAcceptAFixVersionBasedUponLogonMessage()
    {
        connectOtherSession();

        messagesCanBeExchanged();

        acquireOtherAcceptingSession();

        messagesCanBeExchanged();

        assertEquals(OTHER_FIX_DICTIONARY, acceptingSession.fixDictionary().getClass());

        // test field is added to messages with the specified fix dictionary
        initiatingOtfAcceptor.messages().forEach(this::assertHasTestField);
    }

    private void assertHasTestField(final FixMessage msg)
    {
        assertEquals(msg.toString(), TEST_VALUE, msg.get(Constants.TEST_FIELD));
    }

    private void acquireOtherAcceptingSession()
    {
        acquireAcceptingSession(OTHER_INITIATOR_ID);
    }

    private void connectOtherSession()
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials(USERNAME, PASSWORD)
            .senderCompId(OTHER_INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .fixDictionary(OTHER_FIX_DICTIONARY)
            .build();

        final Reply<Session> reply = initiatingLibrary.initiate(config);
        initiatingSession = completeConnectSessions(reply);
    }

    private void bothSessionsCanExchangeMessages()
    {
        messagesCanBeExchanged();

        final FixDictionaryImpl fixtDictionary = new FixDictionaryImpl();

        final String testReqID = testReqId();
        sendTestRequest(fixtInitiatingSession, testReqID, fixtDictionary);
        assertReceivedSingleHeartbeat(testSystem, initiatingOtfAcceptor, testReqID);

        assertHeaderHasApplVerId(initiatingOtfAcceptor);
    }

    private void assertHeaderHasApplVerId(final FakeOtfAcceptor acceptor)
    {
        final FixMessage message = acceptor.lastReceivedMessage();
        final ApplVerID applVerID = ApplVerID.decode(message.get(APPL_VER_ID));
        assertEquals(FIX50, applVerID);
    }

    private void acquireFixTSession()
    {
        final long sessionId = acceptingHandler.awaitSessionId(testSystem::poll);

        fixtAcceptingSession = acquireSession(
            acceptingHandler, acceptingLibrary, sessionId, testSystem);
        assertEquals(INITIATOR_ID, acceptingHandler.lastInitiatorCompId());
        assertEquals(FIXT_ACCEPTOR_ID, acceptingHandler.lastAcceptorCompId());
        assertNotNull("unable to acquire accepting session", fixtAcceptingSession);
    }

    private void connectFixTSessions()
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
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

    public void configureHeader(final SessionHeaderEncoder sessionHeader, final long sessionId)
    {
        if (sessionHeader instanceof HeaderEncoder)
        {
            final HeaderEncoder header = (HeaderEncoder)sessionHeader;
            if (!header.hasApplVerID())
            {
                header.applVerID(applVerID);
            }
        }
        // This is to test that messages are exchanged with the appropriate field
        else if (sessionHeader instanceof uk.co.real_logic.artio.other.builder.HeaderEncoder)
        {
            final uk.co.real_logic.artio.other.builder.HeaderEncoder header =
                (uk.co.real_logic.artio.other.builder.HeaderEncoder)sessionHeader;
            header.testField(MultipleFixVersionSystemTest.TEST_VALUE);
        }
    }
}
