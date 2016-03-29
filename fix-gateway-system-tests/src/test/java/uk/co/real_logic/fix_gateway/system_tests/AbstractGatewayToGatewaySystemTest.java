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

import org.junit.After;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import uk.co.real_logic.fix_gateway.builder.ResendRequestEncoder;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.session.Session;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.TestFixtures.cleanupDirectory;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class AbstractGatewayToGatewaySystemTest
{
    protected int port = unusedPort();
    protected int initAeronPort = unusedPort();

    protected MediaDriver mediaDriver;
    protected FixEngine acceptingEngine;
    protected FixEngine initiatingEngine;
    protected FixLibrary acceptingLibrary;
    protected FixLibrary initiatingLibrary;
    protected Session initiatingSession;
    protected Session acceptingSession;

    protected FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    protected FakeSessionHandler acceptingSessionHandler = new FakeSessionHandler(acceptingOtfAcceptor);

    protected FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    protected FakeSessionHandler initiatingSessionHandler = new FakeSessionHandler(initiatingOtfAcceptor);

    protected void assertOriginalLibraryDoesNotReceiveMessages(final int initiator1MessageCount)
    {
        initiatingLibrary.poll(5);
        assertThat("Messages received by wrong initiator",
            initiatingOtfAcceptor.messages(), hasSize(initiator1MessageCount));
    }

    protected void assertSequenceFromInitToAcceptAt(final int expectedSeqNum)
    {
        assertEquals(expectedSeqNum, initiatingSession.lastSentMsgSeqNum());
        assertEquals(expectedSeqNum, acceptingSession.lastReceivedMsgSeqNum());

        while (initiatingSession.lastReceivedMsgSeqNum() < expectedSeqNum)
        {
            initiatingLibrary.poll(1);
        }

        assertEquals(expectedSeqNum, initiatingSession.lastReceivedMsgSeqNum());
        assertEquals(expectedSeqNum, acceptingSession.lastSentMsgSeqNum());
    }

    protected void assertSessionsDisconnected()
    {
        assertSessionDisconnected(initiatingLibrary, acceptingLibrary, initiatingSession);
        assertSessionDisconnected(acceptingLibrary, initiatingLibrary, acceptingSession);

        assertEventuallyTrue("libraries receive disconnect messages", () ->
        {
            poll(initiatingLibrary, acceptingLibrary);
            assertNotSession(acceptingSessionHandler, acceptingSession);
            assertNotSession(initiatingSessionHandler, initiatingSession);
        });
    }

    protected void assertNotSession(final FakeSessionHandler sessionHandler, final Session session)
    {
        assertThat(sessionHandler.sessions(), not(hasItem(session)));
    }

    protected void connectSessions()
    {
        initiatingSession = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);

        assertConnected(initiatingSession);
        sessionLogsOn(initiatingLibrary, acceptingLibrary, initiatingSession);
        acceptingSession = acceptSession(acceptingSessionHandler, acceptingLibrary);
    }

    protected void assertMessageResent()
    {
        assertThat(acceptingOtfAcceptor.messages(), hasSize(0));
        assertEventuallyTrue("Failed to receive the reply", () ->
        {
            acceptingLibrary.poll(1);
            initiatingLibrary.poll(1);

            final String messageType = acceptingOtfAcceptor.lastMessage().getMessageType();
            assertEquals("0", messageType);
            assertEquals(INITIATOR_ID, acceptingOtfAcceptor.lastSenderCompId());
            assertNull("Detected Error", acceptingOtfAcceptor.lastError());
            assertTrue("Failed to complete parsing", acceptingOtfAcceptor.isCompleted());
        });
    }

    protected void sendResendRequest()
    {
        final int seqNum = acceptingSession.lastReceivedMsgSeqNum();
        final ResendRequestEncoder resendRequest = new ResendRequestEncoder()
            .beginSeqNo(seqNum)
            .endSeqNo(seqNum);

        acceptingOtfAcceptor.messages().clear();

        acceptingSession.send(resendRequest);
    }

    @After
    public void close()
    {
        CloseHelper.close(initiatingLibrary);
        CloseHelper.close(acceptingLibrary);

        CloseHelper.close(initiatingEngine);
        CloseHelper.close(acceptingEngine);

        CloseHelper.close(mediaDriver);
        cleanupDirectory(mediaDriver);
    }

}
