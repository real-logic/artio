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
package uk.co.real_logic.artio.system_tests;

import org.agrona.CloseHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.admin.ArtioAdmin;
import uk.co.real_logic.artio.admin.ArtioAdminConfiguration;
import uk.co.real_logic.artio.admin.FixAdminSession;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ArtioAdminSystemTest extends AbstractGatewayToGatewaySystemTest
{
    // Manage fix sessions:
    //Query session info:
    //Slow consumer status
    //Manage session:
    //Disconnect

    private ArtioAdmin artioAdmin;

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        acceptingEngine = FixEngine.launch(acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID, nanoClock)
            .deleteLogFileDirOnStart(true));

        initiatingEngine = launchInitiatingEngine(libraryAeronPort, nanoClock);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler, nanoClock);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, nanoClock);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);
    }

    @After
    public void teardown()
    {
        CloseHelper.close(artioAdmin);
    }

    @Test
    public void shouldQuerySessionStatus()
    {
        connectSessions();
        acquireAcceptingSession();
        messagesCanBeExchanged();

        final ArtioAdminConfiguration config = new ArtioAdminConfiguration();
        config.libraryAeronChannel(acceptingEngine.configuration().libraryAeronChannel());
        artioAdmin = ArtioAdmin.launch(config);
        assertFalse(artioAdmin.isClosed());

        final List<FixAdminSession> allFixSessions = artioAdmin.allFixSessions();
        assertThat(allFixSessions, hasSize(1));

        final FixAdminSession session = allFixSessions.get(0);
        assertEquals(acceptingSession.connectionId(), session.connectionId());
        assertEquals(acceptingSession.connectedHost(), session.connectedHost());
        assertEquals(acceptingSession.connectedPort(), session.connectedPort());
        assertEquals(acceptingSession.id(), session.sessionId());
        assertEquals(acceptingSession.compositeKey().toString(), session.sessionKey().toString());
        connectTimeRange.assertWithinRange(session.lastLogonTime());
        assertEquals(acceptingSession.lastReceivedMsgSeqNum(), session.lastReceivedMsgSeqNum());
        assertEquals(acceptingSession.lastSentMsgSeqNum(), session.lastSentMsgSeqNum());
        assertTrue(session.isConnected());

        artioAdmin.close();
        assertTrue(artioAdmin.isClosed());

        artioAdmin.close();
        assertTrue("Close not idempotent", artioAdmin.isClosed());
    }

    // TODO: test multiple sessions
    // TODO: test multiple admin API instances.
}
