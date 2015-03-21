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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.framer.session.InitiatorSession;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.framer.session.SessionState.ACTIVE;

public class GatewayIntegrationTest
{

    public static final byte[] EG_MESSAGE = ("8=FIX.4.2\0019=145\00135=D\00134=4\00149=ABC_DEFG01\001" +
        "52=20090323-15:40:29\00156=CCG\001115=XYZ\00111=NF 0542/03232009\00154=1\00138=100\00155=CVS\00140=1" +
        "\00159=0\00147=A\00160=20090323-15:40:29\00121=1\001207=N\00110=194\001").getBytes(StandardCharsets.US_ASCII);

    private MediaDriver mediaDriver;
    private FixGateway acceptingGateway;
    private FixGateway initiatingGateway;
    private InitiatorSession session;
    private FakeOtfAcceptor fakeOtfAcceptor;

    @Before
    public void launch()
    {
        final int port = unusedPort();

        mediaDriver = MediaDriver.launch(new MediaDriver.Context().threadingMode(SHARED));

        fakeOtfAcceptor = new FakeOtfAcceptor();

        final StaticConfiguration acceptingConfig = new StaticConfiguration()
                .registerFallbackAcceptor(fakeOtfAcceptor)
                .bind("localhost", port)
                .aeronChannel("udp://localhost:" + unusedPort());
        acceptingGateway = FixGateway.launch(acceptingConfig);

        final StaticConfiguration initiatingConfig = new StaticConfiguration()
                .bind("localhost", unusedPort())
                .aeronChannel("udp://localhost:" + unusedPort());
        initiatingGateway = FixGateway.launch(initiatingConfig);

        final SessionConfiguration config = SessionConfiguration.builder()
                .address("localhost", port)
                .credentials("bob", "Uv1aegoh")
                .senderCompId("LEH_LZJ02")
                .targetCompId("CCG")
                .build();
        session = initiatingGateway.initiate(config, null);
    }

    @Ignore
    @Test
    public void sessionHasBeenInitiated() throws InterruptedException
    {
        assertTrue("Session has failed to connect", session.isConnected());
        assertTrue("Session has failed to logon", session.state() == ACTIVE);
    }

    @Ignore
    @Test
    public void messagesCanBeSentFromInitiatorToAcceptor() throws InterruptedException
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[8 * 1024]);
        buffer.putBytes(0, EG_MESSAGE);

        session.send(buffer, 0, EG_MESSAGE.length, 'D');
        System.out.println("SENDING");

        assertEventuallyTrue("Failed to receive a message", fakeOtfAcceptor::hasSeenMessage);
        assertEquals(Arrays.asList((int) 'D'), fakeOtfAcceptor.messageTypes());
    }

    // TODO: disconnect an initiating session and verify disconnect message
    // TODO: shutdown a gateway and check logout
    // TODO: initiate/accept multiple sessions

    @After
    public void close() throws Exception
    {
        if (acceptingGateway != null)
        {
            acceptingGateway.close();
        }

        if (initiatingGateway != null)
        {
            initiatingGateway.close();
        }

        if (mediaDriver != null)
        {
            mediaDriver.close();
        }
    }

}
