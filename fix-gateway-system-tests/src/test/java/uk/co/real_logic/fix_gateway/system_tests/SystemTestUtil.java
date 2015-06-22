/*
 * Copyright 2015 Real Logic Ltd.
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

import org.hamcrest.Matcher;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.auth.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.auth.CompIdAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.auth.SenderIdAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.session.InitiatorSession;
import uk.co.real_logic.fix_gateway.session.NewSessionHandler;
import uk.co.real_logic.fix_gateway.session.Session;

import java.io.File;
import java.util.Arrays;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyEquals;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;

public final class SystemTestUtil
{
    public static final long CONNECTION_ID = 0L;
    public static final String ACCEPTOR_ID = "CCG";
    public static final String INITIATOR_ID = "LEH_LZJ02";

    public static MediaDriver launchMediaDriver()
    {
        return MediaDriver.launch(new MediaDriver.Context().threadingMode(SHARED));
    }

    public static void assertDisconnected(final FakeSessionHandler sessionHandler, final Session session)
    {
        assertSessionDisconnected(session);

        assertAcceptorDisconnected(sessionHandler);
    }

    public static void assertAcceptorDisconnected(final FakeSessionHandler sessionHandler)
    {
        assertEventuallyTrue("Failed to disconnect",
            () ->
            {
                sessionHandler.poll();
                assertEquals(CONNECTION_ID, sessionHandler.connectionId());
            });
    }

    private static void assertSessionDisconnected(final Session session)
    {
        assertEventuallyTrue("Session is still connected", () -> !session.isConnected());
    }

    public static void sendTestRequest(final Session session)
    {
        assertEventuallyTrue("Session not connected", session::isConnected);

        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("hi");

        session.send(testRequest);
    }

    public static void assertReceivedMessage(
        final FakeSessionHandler sessionHandler, final FakeOtfAcceptor acceptor)
    {
        assertEventuallyEquals("Failed to receive a logon and test request message", 2, sessionHandler::poll);
        assertEquals(2, acceptor.messageTypes().size());
        assertThat(acceptor.messageTypes(), hasItem(TestRequestDecoder.MESSAGE_TYPE));
    }

    public static <T> Matcher<Iterable<? super T>> containsInitiator()
    {
        return containsLogon(ACCEPTOR_ID, INITIATOR_ID);
    }

    public static <T> Matcher<Iterable<? super T>> containsAcceptor()
    {
        return containsLogon(INITIATOR_ID, ACCEPTOR_ID);
    }

    private static <T> Matcher<Iterable<? super T>> containsLogon(final String senderCompId, final String targetCompId)
    {
        return hasItem(
            allOf(hasSenderCompId(senderCompId),
                  hasTargetCompId(targetCompId)));
    }

    private static <T> Matcher<T> hasTargetCompId(final String targetCompId)
    {
        return hasProperty("targetCompID", equalTo(targetCompId));
    }

    private static <T> Matcher<T> hasSenderCompId(final String senderCompId)
    {
        return hasProperty("senderCompID", equalTo(senderCompId));
    }

    public static InitiatorSession initiate(
        final FixGateway gateway,
        final int port,
        final String initiatorId,
        final String acceptorId)
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials("bob", "Uv1aegoh")
            .senderCompId(initiatorId)
            .targetCompId(acceptorId)
            .build();
        return gateway.initiate(config);
    }

    public static FixGateway launchInitiatingGateway(final NewSessionHandler sessionHandler)
    {
        final StaticConfiguration initiatingConfig = new StaticConfiguration()
            .bind("localhost", unusedPort())
            .aeronChannel("udp://localhost:" + unusedPort())
            .newSessionHandler(sessionHandler)
            .counterBuffersFile(IoUtil.tmpDirName() + "fix-client" + File.separator + "counters")
            .logFileDir("client-logs");
        return FixGateway.launch(initiatingConfig);
    }

    public static FixGateway launchAcceptingGateway(
        final int port,
        final NewSessionHandler sessionHandler,
        final String acceptorId,
        final String initiatorId)
    {
        final AuthenticationStrategy authenticationStrategy = new CompIdAuthenticationStrategy(acceptorId)
                .and(new SenderIdAuthenticationStrategy(Arrays.asList(initiatorId)));

        final StaticConfiguration acceptingConfig = new StaticConfiguration()
            .bind("localhost", port)
            .aeronChannel("udp://localhost:" + unusedPort())
            .authenticationStrategy(authenticationStrategy)
            .newSessionHandler(sessionHandler)
            .counterBuffersFile(IoUtil.tmpDirName() + "fix-acceptor" + File.separator + "counters")
            .logFileDir("acceptor-logs");
        return FixGateway.launch(acceptingConfig);
    }

}
