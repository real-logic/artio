package uk.co.real_logic.artio.system_tests;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.EpochNanoClock;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.artio.decoder.AbstractResendRequestDecoder;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.fields.EpochFractionFormat;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.session.DirectSessionProxy;
import uk.co.real_logic.artio.session.ResendRequestResponse;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.session.SessionCustomisationStrategy;
import uk.co.real_logic.artio.session.SessionIdStrategy;
import uk.co.real_logic.artio.session.SessionProxy;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.DebugFIXClient;
import uk.co.real_logic.artio.util.DebugServer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner.SOLE_LIBRARY;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.ACCEPTOR_ID;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.INITIATOR_ID;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.acceptingConfig;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.acceptingLibraryConfig;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.connect;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.initiatingConfig;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.initiatingLibraryConfig;

/**
 * Reproduce race (issue #503) while sending ResendRequest and ResetSequence when both
 * parties detect a gap on Logon.
 * <p>
 * Also reproduces the fact that SessionProxy is not invoked when a ResetSequence message is sent during replay.
 */
public class RaceResendResetTest extends AbstractGatewayToGatewaySystemTest
{
    private boolean useProxy;
    private boolean sendResendRequestCalled;
    private boolean sendSequenceResetCalled;

    /**
     * When positive, simulate a SessionProxy that sends outbound FIX messages asynchronously,
     * through an external cluster.
     */
    private long sleepBeforeSendResendRequest;

    private final ArrayList<AutoCloseable> autoClose = new ArrayList<>();

    private void launch()
    {
        mediaDriver = launchMediaDriver();
        launchAccepting();
        launchInitiating();
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);
    }

    private void launchInitiating()
    {
        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort, nanoClock)
            .deleteLogFileDirOnStart(true)
            .initialAcceptedSessionOwner(SOLE_LIBRARY);
        initiatingEngine = FixEngine.launch(initiatingConfig);
        final LibraryConfiguration lib = initiatingLibraryConfig(libraryAeronPort, initiatingHandler, nanoClock);
        if (useProxy)
        {
            lib.sessionProxyFactory(this::sessionProxyFactory);
        }
        initiatingLibrary = connect(lib);
    }

    static class PendingResendRequest
    {
        final Session session;
        final MutableAsciiBuffer message;
        final int beginSeqNo;
        final int endSeqNo;

        PendingResendRequest(
            final Session session, final int beginSeqNo, final int endSeqNo, final MutableAsciiBuffer message
        )
        {
            this.session = session;
            this.beginSeqNo = beginSeqNo;
            this.endSeqNo = endSeqNo;
            this.message = message;
        }

        public void execute()
        {
            System.err.println("Execute resend request");
            session.executeResendRequest(beginSeqNo, endSeqNo, message, 0, message.capacity());
        }
    }

    private void launchAccepting()
    {
        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID, nanoClock)
            .deleteLogFileDirOnStart(true)
            .initialAcceptedSessionOwner(SOLE_LIBRARY);
        acceptingEngine = FixEngine.launch(acceptingConfig);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler, nanoClock);
        acceptingLibrary = connect(acceptingLibraryConfig);
    }

    /**
     * Sanity check that we can connect Artio to a debug server with canned messages.
     */
    @Test
    public void testDebugServer() throws IOException
    {
        final DebugServer srv = new DebugServer(port);
        srv.setWaitForData(true);
        srv.addFIXResponse(
            "8=FIX.4.4|9=94|35=A|49=acceptor|56=initiator|34=1|52=***|98=0|108=10|141=N|35002=0|35003=0|10=024|"
        );
        srv.start();

        mediaDriver = launchMediaDriver();
        launchInitiating();
        testSystem = new TestSystem(initiatingLibrary);
        connectAndAcquire();
    }

    class Proxy extends DirectSessionProxy
    {
        /**
         * Stores details of received ResendRequest while we wait for ours to be sent.
         */
        private PendingResendRequest pendingResendRequest;

        Proxy(
            final int sessionBufferSize, final GatewayPublication gatewayPublication,
            final SessionIdStrategy sessionIdStrategy, final SessionCustomisationStrategy customisationStrategy,
            final EpochNanoClock clock, final long connectionId, final int libraryId,
            final ErrorHandler errorHandler, final EpochFractionFormat epochFractionPrecision
        )
        {
            super(sessionBufferSize, gatewayPublication, sessionIdStrategy, customisationStrategy, clock, connectionId,
                libraryId, errorHandler, epochFractionPrecision);
        }

        @Override
        public long onResend(
            final Session session, final AbstractResendRequestDecoder resendRequest,
            final int correctedEndSeqNo, final ResendRequestResponse response,
            final AsciiBuffer messageBuffer, final int messageOffset, final int messageLength
        )
        {
            onResendRequestReceived(session, resendRequest, correctedEndSeqNo, response,
                messageBuffer, messageOffset, messageLength);
            return 1;
        }

        private void onResendRequestReceived(
            final Session session, final AbstractResendRequestDecoder request, final int endSeqNo,
            final ResendRequestResponse response,
            final AsciiBuffer messageBuffer, final int messageOffset, final int messageLength
        )
        {
            System.err.println("onResendRequestReceived() called");
            if (!useProxy || sleepBeforeSendResendRequest == 0)
            {
                response.resend();
            }
            else
            {
                response.delay();
                final MutableAsciiBuffer buf = new MutableAsciiBuffer(new byte[messageLength]);
                buf.putBytes(0, messageBuffer, messageOffset, messageLength);
                pendingResendRequest = new PendingResendRequest(session, request.beginSeqNo(), endSeqNo, buf);
            }
        }

        @Override
        public long sendResendRequest(
            final int msgSeqNo,
            final int beginSeqNo,
            final int endSeqNo,
            final int sequenceIndex,
            final int lastMsgSeqNumProcessed)
        {
            System.err.println("sendResendRequest called with msgSeqNo = " + msgSeqNo);
            sendResendRequestCalled = true;
            if (sleepBeforeSendResendRequest > 0)
            {
                new Thread(() ->
                {
                    try
                    {
                        Thread.sleep(sleepBeforeSendResendRequest);
                    }
                    catch (final InterruptedException ignored)
                    {
                    }
                    System.err.println("Executing super.sendResendRequest() after delay: msgSeqNo = " + msgSeqNo);
                    super.sendResendRequest(msgSeqNo, beginSeqNo, endSeqNo, sequenceIndex, lastMsgSeqNumProcessed);
                    if (pendingResendRequest != null)
                    {
                        pendingResendRequest.execute();
                    }
                    else
                    {
                        System.err.println("onResend not called (async)");
                    }
                }).start();
            }
            else
            {
                System.err.println("Directly executing sendResendRequest msgSeqNo = " + msgSeqNo);
                super.sendResendRequest(msgSeqNo, beginSeqNo, endSeqNo, sequenceIndex, lastMsgSeqNumProcessed);
                if (pendingResendRequest != null)
                {
                    pendingResendRequest.execute();
                }
                else
                {
                    System.err.println("onResend not called (direct)");
                }
            }
            return 1;
        }

        @Override
        public long sendSequenceReset(
            final int msgSeqNo,
            final int newSeqNo,
            final int sequenceIndex,
            final int lastMsgSeqNumProcessed)
        {
            sendSequenceResetCalled = true;
            return super.sendSequenceReset(msgSeqNo, newSeqNo, sequenceIndex, lastMsgSeqNumProcessed);
        }

        @Override
        public boolean isAsync()
        {
            return true;
        }
    }

    private SessionProxy sessionProxyFactory(
        final int sessionBufferSize,
        final GatewayPublication gatewayPublication,
        final SessionIdStrategy sessionIdStrategy,
        final SessionCustomisationStrategy customisationStrategy,
        final EpochNanoClock clock,
        final long connectionId,
        final int libraryId,
        final ErrorHandler errorHandler,
        final EpochFractionFormat epochFractionPrecision)
    {
        return new Proxy(sessionBufferSize, gatewayPublication, sessionIdStrategy, customisationStrategy,
            clock, connectionId, libraryId, errorHandler, epochFractionPrecision);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldNotInvertResendAndResetNoProxy() throws Exception
    {
        useProxy = false;
        reconnectTest();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSendResendBeforeResetSyncProxy() throws Exception
    {
        useProxy = true;
        sleepBeforeSendResendRequest = 0;
        reconnectTest();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSendResendBeforeResetAsyncProxy() throws Exception
    {
        useProxy = true;
        sleepBeforeSendResendRequest = 100;
        reconnectTest();
    }

    @Ignore // SequenceReset is directly sent by replayer, does not go through SessionProxy
    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldCallProxySendSequenceReset() throws Exception
    {
        useProxy = true;
        reconnectTest();
        assertTrue("SessionProxy.sendResendRequest() not called", sendResendRequestCalled);
        assertTrue("SessionProxy.sendSequenceReset() not called", sendSequenceResetCalled);
    }

    private void reconnectTest() throws Exception
    {
        launch();

        connectAndAcquire();

        messagesCanBeExchanged();

        disconnectSessions();
        Exceptions.closeAll(this::closeAcceptingEngine);

        assertEquals(3, acceptingSession.lastReceivedMsgSeqNum());
        assertEquals(3, initiatingSession.lastReceivedMsgSeqNum());

        final DebugServer srv = new DebugServer(port);
        srv.setWaitForData(true);
        srv.addFIXResponse(
            "8=FIX.4.4|9=94|35=A|49=acceptor|56=initiator|34=5|52=***|98=0|108=10|141=N|35002=0|35003=0|10=024|",
            "8=FIX.4.4|9=94|35=2|49=acceptor|56=initiator|34=6|52=***|7=4|16=0|10=024|"
        );
        srv.start();
        autoClose.add(srv::stop);

        connectPersistentSessions(4, 4, false);

        final DebugFIXClient exchange = new DebugFIXClient(srv.popClient(5000));
        autoClose.add(exchange::close);
        exchange.popAndAssert("35=A 34=4");
        exchange.popAndAssert("35=2 34=5 7=4 16=0"); // ResendRequest now always received first
        exchange.popAndAssert("35=4 34=4 36=6");
    }

    @Override
    public void close()
    {
        for (final AutoCloseable autoCloseable : autoClose)
        {
            try
            {
                autoCloseable.close();
            }
            catch (final Exception ignored)
            {
            }
        }
        super.close();
    }

    private void connectAndAcquire()
    {
        connectSessions();
        acceptingSession = acceptingHandler.lastSession();
    }
}
