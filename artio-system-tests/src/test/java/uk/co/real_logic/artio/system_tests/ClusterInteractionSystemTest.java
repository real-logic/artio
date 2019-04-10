/*
 * Copyright 2015-2019 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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

import org.agrona.concurrent.EpochClock;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Timing;
import uk.co.real_logic.artio.builder.HeaderEncoder;
import uk.co.real_logic.artio.builder.HeartbeatEncoder;
import uk.co.real_logic.artio.builder.LogonEncoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.fields.RejectReason;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.session.*;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ClusterInteractionSystemTest extends AbstractGatewayToGatewaySystemTest
{
    private final FakeConnectHandler fakeConnectHandler = new FakeConnectHandler();
    private final FakeSessionProxy fakeSessionProxy = new FakeSessionProxy();
    private FollowerSession acceptingFollowerSession = null;
    private FakeHandler acceptingHandler = new FakeHandler(acceptingOtfAcceptor)
    {
        @Override
        public SessionHandler onSessionAcquired(final Session session, final boolean isSlow)
        {
            acceptingFollowerSession = acceptingLibrary.followerSession(
                session.id(),
                session.connectionId(),
                session.sequenceIndex());

            return super.onSessionAcquired(session, isSlow);
        }
    };

    private int sessionProxyRequests = 0;

    @Before
    public void launch()
    {
        delete(ACCEPTOR_LOGS);

        mediaDriver = launchMediaDriver();

        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID);
        acceptingConfig.soleLibraryMode(true);

        acceptingEngine = FixEngine.launch(acceptingConfig);
        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler)
            .libraryConnectHandler(fakeConnectHandler)
            .sessionProxyFactory(this::sessionProxyFactory);

        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }

    @Test(timeout = 10_000L)
    public void shouldRoundTripMessagesViaCluster()
    {
        awaitForwardingOfAcceptingSession();

        assertNotNull(acceptingFollowerSession);
        assertEquals(1, sessionProxyRequests);

        messagesCanBeExchanged();

        assertEquals(1, fakeSessionProxy.sentHeartbeats);

        assertInitiatingSequenceIndexIs(0);
    }

    // TODO: messages stored via followerSession can be read afterwards.
    // TODO: restarting connections and failover

    private void awaitForwardingOfAcceptingSession()
    {
        Timing.assertEventuallyTrue(
            "Couldn't acquire session",
            () ->
            {
                testSystem.poll();
                acceptingSession = acceptingHandler.lastSession();

                return acceptingSession != null;
            });

        acceptingHandler.resetSession();
        final CompositeKey compositeKey = acceptingSession.compositeKey();
        assertEquals(INITIATOR_ID, compositeKey.remoteCompId());
        assertEquals(ACCEPTOR_ID, compositeKey.localCompId());
        assertNotNull("unable to acquire accepting session", acceptingSession);
    }

    private SessionProxy sessionProxyFactory(
        final int sessionBufferSize,
        final GatewayPublication gatewayPublication,
        final SessionIdStrategy sessionIdStrategy,
        final SessionCustomisationStrategy customisationStrategy,
        final EpochClock clock,
        final long connectionId,
        final int libraryId)
    {
        sessionProxyRequests++;
        return fakeSessionProxy;
    }

    class FakeSessionProxy implements SessionProxy
    {
        private final SessionIdStrategy sessionIdStrategy = SessionIdStrategy.senderAndTarget();
        private final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
        private final HeartbeatEncoder heartbeat = new HeartbeatEncoder();
        private final LogonEncoder logon = new LogonEncoder();
        private final List<HeaderEncoder> headers = asList(logon.header(), heartbeat.header());

        private int sentHeartbeats = 0;
        private boolean seqNumResetRequested = false;

        public void setupSession(final long sessionId, final CompositeKey sessionKey)
        {
            requireNonNull(sessionKey, "sessionKey");

            for (final HeaderEncoder header : headers)
            {
                sessionIdStrategy.setupSession(sessionKey, header);
            }
        }

        public long resendRequest(
            final int msgSeqNo,
            final int beginSeqNo,
            final int endSeqNo,
            final int sequenceIndex,
            final int lastMsgSeqNumProcessed)
        {
            return 0;
        }

        public long requestDisconnect(final long connectionId, final DisconnectReason reason)
        {
            return 0;
        }

        public long logon(
            final int heartbeatIntervalInS,
            final int msgSeqNo,
            final String username,
            final String password,
            final boolean resetSeqNumFlag,
            final int sequenceIndex,
            final int lastMsgSeqNumProcessed)
        {
            final HeaderEncoder header = logon.header();
            setupHeader(header, msgSeqNo);

            logon
                .heartBtInt(heartbeatIntervalInS)
                .resetSeqNumFlag(resetSeqNumFlag)
                .encryptMethod(0);

            if (notNullOrEmpty(username))
            {
                logon.username(username);
            }
            if (notNullOrEmpty(password))
            {
                logon.password(password);
            }
            seqNumResetRequested = logon.resetSeqNumFlag();

            return acceptingFollowerSession.send(logon, msgSeqNo);
        }

        public long logout(final int msgSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
        {
            return 0;
        }

        public long logout(
            final int msgSeqNo,
            final int sequenceIndex,
            final int rejectReason,
            final int lastMsgSeqNumProcessed)
        {
            return 0;
        }

        public long lowSequenceNumberLogout(
            final int msgSeqNo,
            final int expectedSeqNo,
            final int receivedSeqNo,
            final int sequenceIndex,
            final int lastMsgSeqNumProcessed)
        {
            return 0;
        }

        public long incorrectBeginStringLogout(
            final int msgSeqNo,
            final int sequenceIndex,
            final int lastMsgSeqNumProcessed)
        {
            return 0;
        }

        public long negativeHeartbeatLogout(
            final int msgSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
        {
            return 0;
        }

        public long receivedMessageWithoutSequenceNumber(
            final int msgSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
        {
            return 0;
        }

        public long rejectWhilstNotLoggedOn(
            final int msgSeqNo, final RejectReason reason, final int sequenceIndex, final int lastMsgSeqNumProcessed)
        {
            return 0;
        }

        public long heartbeat(final int msgSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
        {
            return 0;
        }

        public long heartbeat(
            final char[] testReqId,
            final int testReqIdLength,
            final int msgSeqNo,
            final int sequenceIndex,
            final int lastMsgSeqNumProcessed)
        {
            sentHeartbeats++;

            final HeaderEncoder header = heartbeat.header();
            setupHeader(header, msgSeqNo);

            if (testReqId != null)
            {
                heartbeat.testReqID(testReqId, testReqIdLength);
            }
            else
            {
                heartbeat.resetTestReqID();
            }

            return acceptingFollowerSession.send(heartbeat, msgSeqNo);
        }

        public long reject(
            final int msgSeqNo,
            final int refSeqNum,
            final int refTagId,
            final char[] refMsgType,
            final int refMsgTypeLength,
            final int rejectReason,
            final int sequenceIndex,
            final int lastMsgSeqNumProcessed)
        {
            return 0;
        }

        public long testRequest(
            final int msgSeqNo, final CharSequence testReqID, final int sequenceIndex, final int lastMsgSeqNumProcessed)
        {
            return 0;
        }

        public long sequenceReset(
            final int msgSeqNo, final int newSeqNo, final int sequenceIndex, final int lastMsgSeqNumProcessed)
        {
            return 0;
        }

        public void libraryConnected(final boolean libraryConnected)
        {

        }

        public boolean seqNumResetRequested()
        {
            return seqNumResetRequested;
        }

        private void setupHeader(final HeaderEncoder header, final int msgSeqNo)
        {
            final UtcTimestampEncoder timestampEncoder = this.timestampEncoder;
            header.sendingTime(timestampEncoder.buffer(), timestampEncoder.update(System.currentTimeMillis()));
            header.msgSeqNum(msgSeqNo);
        }

        private boolean notNullOrEmpty(final String string)
        {
            return string != null && string.length() > 0;
        }
    }
}
