/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.system_tests;

import io.aeron.driver.MediaDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.session.Session;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.Reply.State.COMPLETED;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.messages.SessionState.ACTIVE;
import static uk.co.real_logic.artio.messages.SessionState.AWAITING_RESEND;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

// For reproducing error scenarios when initiating a connection
public class MessageBasedInitiatorSystemTest
{
    private static final int LOGON_SEQ_NUM = 2;

    private final FakeOtfAcceptor otfAcceptor = new FakeOtfAcceptor();
    private final FakeHandler handler = new FakeHandler(otfAcceptor);
    private final int fixPort = unusedPort();
    private final int libraryAeronPort = unusedPort();

    private MediaDriver mediaDriver;
    private FixEngine engine;
    private FixLibrary library;
    private TestSystem testSystem;
    private int polled;

    private Reply<Session> sessionReply;

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();
        engine = launchInitiatingEngine(libraryAeronPort);
        testSystem = new TestSystem();
        library = testSystem.connect(initiatingLibraryConfig(libraryAeronPort, handler));
    }

    @Test
    public void shouldRequestResendForWrongSequenceNumber() throws IOException
    {
        try (FixConnection connection = acceptConnection())
        {
            sendLogonToAcceptor(connection);

            connection.msgSeqNum(LOGON_SEQ_NUM).logon(false);

            final Reply<Session> reply = testSystem.awaitReply(this.sessionReply);
            assertEquals(COMPLETED, reply.state());

            final Session session = reply.resultIfPresent();
            assertEquals(AWAITING_RESEND, session.state());
        }
    }

    @Test
    public void shouldCompleteInitiateWhenResetSeqNumFlagSet() throws IOException
    {
        try (FixConnection connection = acceptConnection())
        {
            sendLogonToAcceptor(connection);

            connection.logon(true);

            final Reply<Session> reply = testSystem.awaitReply(this.sessionReply);
            assertEquals(COMPLETED, reply.state());

            final Session session = reply.resultIfPresent();
            assertEquals(ACTIVE, session.state());
            assertEquals(1, session.lastReceivedMsgSeqNum());
        }
    }

    private void sendLogonToAcceptor(final FixConnection connection)
    {
        assertEventuallyTrue(
            "Never sent logon", () ->
            {
                polled += library.poll(LIBRARY_LIMIT);
                return polled > 2;
            });

        connection.readLogonReply();
    }

    private FixConnection acceptConnection() throws IOException
    {
        return FixConnection.accept(fixPort, () ->
            sessionReply = SystemTestUtil.initiate(library, fixPort, INITIATOR_ID, ACCEPTOR_ID));
    }

    @After
    public void tearDown()
    {
        Exceptions.closeAll(library, engine);
        cleanupMediaDriver(mediaDriver);
    }
}
