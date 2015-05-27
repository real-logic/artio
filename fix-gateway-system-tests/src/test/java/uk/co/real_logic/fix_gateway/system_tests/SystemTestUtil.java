package uk.co.real_logic.fix_gateway.system_tests;

import org.hamcrest.Matcher;
import quickfix.*;
import quickfix.field.BeginString;
import quickfix.field.MsgType;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.auth.CompIdAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.session.InitiatorSession;
import uk.co.real_logic.fix_gateway.session.NewSessionHandler;
import uk.co.real_logic.fix_gateway.session.Session;

import java.io.File;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static quickfix.field.MsgType.TEST_REQUEST;
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
        throws InterruptedException
    {
        assertSessionDisconnected(session);

        assertAcceptorDisconnected(sessionHandler);
    }

    public static void assertAcceptorDisconnected(final FakeSessionHandler sessionHandler)
    {
        assertEventuallyTrue("Failed to disconnect",
            () ->
            {
                sessionHandler.subscription().poll(1);
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
        final Subscription subscription, final FakeOtfAcceptor acceptor)
    {
        assertEventuallyEquals("Failed to receive a logon and test request message", 2, () -> subscription.poll(2));
        assertEquals(2, acceptor.messageTypes().size());
        assertThat(acceptor.messageTypes(), hasItem(TestRequestDecoder.MESSAGE_TYPE));
    }

    public static void assertQuickFixDisconnected(
        final FakeQuickFixApplication acceptor,
        final Matcher<Iterable<? super SessionID>> sessionMatcher)
    {
        assertEventuallyEquals("Failed to receive a logout", 1, () -> acceptor.logouts().size());
        final List<SessionID> logouts = acceptor.logouts();
        DebugLogger.log("\nLogouts: %s\n", logouts);
        assertThat(logouts, sessionMatcher);
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
        return gateway.initiate(config, null);
    }

    public static FixGateway launchInitiatingGateway(final NewSessionHandler sessionHandler)
    {
        final StaticConfiguration initiatingConfig = new StaticConfiguration()
            .bind("localhost", unusedPort())
            .aeronChannel("udp://localhost:" + unusedPort())
            .newSessionHandler(sessionHandler)
            .counterBuffersFile(IoUtil.tmpDirName() + "fix-initiator" + File.separator + "counters")
            .logFileDir("initiator-logs");
        return FixGateway.launch(initiatingConfig);
    }

    public static FixGateway launchAcceptingGateway(
        final int port,
        final NewSessionHandler sessionHandler,
        final String acceptorId)
    {
        final StaticConfiguration acceptingConfig = new StaticConfiguration()
            .bind("localhost", port)
            .aeronChannel("udp://localhost:" + unusedPort())
            .authenticationStrategy(new CompIdAuthenticationStrategy(acceptorId))
            .newSessionHandler(sessionHandler)
            .counterBuffersFile(IoUtil.tmpDirName() + "fix-acceptor" + File.separator + "counters")
            .logFileDir("acceptor-logs");
        return FixGateway.launch(acceptingConfig);
    }

    public static SocketAcceptor launchQuickFixAcceptor(final int port, final FakeQuickFixApplication application)
        throws ConfigError
    {
        final SessionID sessionID = sessionID(ACCEPTOR_ID, INITIATOR_ID);
        final SessionSettings settings = sessionSettings(sessionID);

        settings.setString("SocketAcceptPort", String.valueOf(port));
        settings.setString(sessionID, "ConnectionType", "acceptor");

        final FileStoreFactory storeFactory = new FileStoreFactory(settings);
        final LogFactory logFactory = new ScreenLogFactory(settings);
        final SocketAcceptor socketAcceptor = new SocketAcceptor(
            application, storeFactory, settings, logFactory, new DefaultMessageFactory());
        socketAcceptor.start();

        return socketAcceptor;
    }

    public static SocketInitiator launchQuickFixInitiator(
        final int port, final FakeQuickFixApplication application) throws ConfigError
    {
        final SessionID sessionID = sessionID(INITIATOR_ID, ACCEPTOR_ID);
        final SessionSettings settings = sessionSettings(sessionID);

        settings.setString("HeartBtInt", "30");

        settings.setString(sessionID, "ConnectionType", "initiator");
        settings.setString(sessionID, "SocketConnectPort", String.valueOf(port));
        settings.setString(sessionID, "SocketConnectHost", "localhost");

        final FileStoreFactory storeFactory = new FileStoreFactory(settings);
        final LogFactory logFactory = new ScreenLogFactory(settings);
        final SocketInitiator socketInitiator = new SocketInitiator(
            application, storeFactory, settings, logFactory, new DefaultMessageFactory());
        socketInitiator.start();
        return socketInitiator;
    }

    private static SessionSettings sessionSettings(final SessionID sessionID)
    {
        final SessionSettings settings = new SessionSettings();
        final String path = "build/tmp/quickfix";
        IoUtil.delete(new File(path), true);
        settings.setString("FileStorePath", path);
        settings.setString("DataDictionary", "FIX44.xml");
        settings.setString("BeginString", "FIX.4.4");
        settings.setString(sessionID, "StartTime", "00:00:00");
        settings.setString(sessionID, "EndTime", "00:00:00");
        return settings;
    }

    private static SessionID sessionID(final String senderCompId, final String targetCompId)
    {
        return new SessionID(
            new BeginString("FIX.4.4"),
            new SenderCompID(senderCompId),
            new TargetCompID(targetCompId)
        );
    }

    static void assertQuickFixReceivedMessage(final FakeQuickFixApplication acceptor)
    {
        assertEventuallyTrue("Unable to fnd test request", () ->
        {
            final List<Message> messages = acceptor.messages();
            for (final Message message : messages)
            {
                if (TEST_REQUEST.equals(getMsgType(message)))
                {
                    return true;
                }
            }

            return false;
        });
    }

    private static String getMsgType(final Message message)
    {
        try
        {
            return message.getHeader().getField(new MsgType()).getValue();
        }
        catch (final FieldNotFound ex)
        {
            ex.printStackTrace();
            return null;
        }
    }
}
