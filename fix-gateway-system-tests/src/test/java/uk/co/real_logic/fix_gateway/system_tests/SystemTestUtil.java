package uk.co.real_logic.fix_gateway.system_tests;

import org.hamcrest.Matcher;
import quickfix.SessionID;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.admin.NewSessionHandler;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.framer.session.InitiatorSession;
import uk.co.real_logic.fix_gateway.framer.session.Session;
import uk.co.real_logic.fix_gateway.replication.GatewaySubscription;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    public static void assertDisconnected(
        final FakeSessionHandler sessionHandler, final Session session) throws InterruptedException
    {
        assertFalse("Session is still connected", session.isConnected());

        assertEventuallyTrue("Failed to disconnect",
            () ->
            {
                sessionHandler.subscription().poll(1);
                assertEquals(CONNECTION_ID, sessionHandler.connectionId());
            });
    }

    public static void sendTestRequest(final Session session)
    {
        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("hi");

        session.send(testRequest);
    }

    public static void assertReceivedMessage(
        final GatewaySubscription subscription, final FakeOtfAcceptor acceptor) throws InterruptedException
    {
        assertEventuallyEquals("Failed to receive a message", 2, () -> subscription.poll(2));
        assertEquals(2, acceptor.messageTypes().size());
        assertThat(acceptor.messageTypes(), hasItem(TestRequestDecoder.MESSAGE_TYPE));
    }

    static void assertQuickFixDisconnected(final FakeQuickFixApplication acceptor)
    {
        assertThat(acceptor.logouts(), containsInitiator());
    }

    static Matcher<Iterable<? extends SessionID>> containsInitiator()
    {
        return contains(
            allOf(hasProperty("senderCompID", equalTo(ACCEPTOR_ID)),
                hasProperty("targetCompID", equalTo(INITIATOR_ID))));
    }

    public static InitiatorSession initiate(final FixGateway gateway, final int port)
    {
        final SessionConfiguration config = SessionConfiguration.builder()
                .address("localhost", port)
                .credentials("bob", "Uv1aegoh")
                .senderCompId(INITIATOR_ID)
                .targetCompId(ACCEPTOR_ID)
                .build();
        return gateway.initiate(config, null);
    }

    public static FixGateway launchInitiatingGateway(final NewSessionHandler sessionHandler)
    {
        final StaticConfiguration initiatingConfig = new StaticConfiguration()
                .bind("localhost", unusedPort())
                .aeronChannel("udp://localhost:" + unusedPort())
                .newSessionHandler(sessionHandler);
        return FixGateway.launch(initiatingConfig);
    }
}
