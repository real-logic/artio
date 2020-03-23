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
package uk.co.real_logic.artio.ilink;

import iLinkBinary.KeepAliveLapsed;
import iLinkBinary.NewOrderSingle514Encoder;
import iLinkBinary.Sequence506Decoder;
import iLinkBinary.SideReq;
import io.aeron.archive.ArchivingMediaDriver;
import org.agrona.CloseHelper;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.library.*;
import uk.co.real_logic.artio.system_tests.TestSystem;

import java.io.IOException;
import java.util.function.LongSupplier;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.ilink.ILink3TestServer.*;
import static uk.co.real_logic.artio.library.ILink3Session.NOT_AWAITING_RETRANSMIT;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ILink3SystemTest
{
    private static final int TEST_KEEP_ALIVE_INTERVAL_IN_MS = 500;

    static final String ACCESS_KEY_ID = "12345678901234567890";
    static final String SESSION_ID = "ABC";
    static final String FIRM_ID = "DEFGH";
    static final String USER_KEY = "somethingprivate";
    public static final String CL_ORD_ID = "123";

    private FakeILink3SessionHandler handler = spy(new FakeILink3SessionHandler(NotAppliedResponse::gapfill));

    private int port = unusedPort();
    private ArchivingMediaDriver mediaDriver;
    private TestSystem testSystem;
    private FixEngine engine;
    private FixLibrary library;
    private ILink3TestServer testServer;
    private Reply<ILink3Session> reply;
    private ILink3Session session;

    public void launch(final boolean printErrorMessages)
    {
        delete(CLIENT_LOGS);

        mediaDriver = launchMediaDriver();

        final EngineConfiguration engineConfig = new EngineConfiguration()
            .logFileDir(CLIENT_LOGS)
            .scheduler(new LowResourceEngineScheduler())
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .libraryAeronChannel(IPC_CHANNEL)
            .printErrorMessages(printErrorMessages)
            .lookupDefaultAcceptorfixDictionary(false);
        engine = FixEngine.launch(engineConfig);

        testSystem = new TestSystem();

        final LibraryConfiguration libraryConfig = new LibraryConfiguration()
            .libraryAeronChannels(singletonList(IPC_CHANNEL))
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS);
        library = testSystem.connect(libraryConfig);
    }

    @After
    public void close()
    {
        testSystem.awaitBlocking(() -> CloseHelper.close(engine));
        CloseHelper.close(library);
        cleanupMediaDriver(mediaDriver);
    }

    @Test
    public void shouldEstablishConnectionAtBeginningOfWeek() throws IOException
    {
        launch(true);

        final ILink3SessionConfiguration sessionConfiguration = sessionConfiguration();

        connectToTestServer(sessionConfiguration);

        readNegotiate();
        testServer.writeNegotiateResponse();

        readEstablish();
        testServer.writeEstablishmentAck();

        testSystem.awaitCompletedReplies(reply);
        session = reply.resultIfPresent();
        assertNotNull(session);

        assertEquals(session.state(), ILink3Session.State.ESTABLISHED);
        assertEquals(testServer.uuid(), session.uuid());
    }

    @Test
    public void shouldSupportInitiatorTerminateConnection() throws IOException
    {
        shouldEstablishConnectionAtBeginningOfWeek();

        terminateAndDisconnect();
    }

    @Test
    public void shouldAcceptExchangeInitiatedTerminate() throws IOException
    {
        shouldEstablishConnectionAtBeginningOfWeek();

        testServer.writeTerminate();

        testSystem.awaitUnbind(session);

        testServer.readTerminate();

        assertDisconnected();
    }

    @Test
    public void shouldExchangeBusinessMessage() throws IOException
    {
        shouldEstablishConnectionAtBeginningOfWeek();

        sendNewOrderSingle();

        testServer.readNewOrderSingle(1);

        terminateAndDisconnect();
    }

    @Test
    public void shouldProvideErrorUponConnectionFailure()
    {
        launch(false);

        final ILink3SessionConfiguration sessionConfiguration = sessionConfiguration();

        reply = library.initiate(sessionConfiguration);
        assertConnectError(containsString("UNABLE_TO_CONNECT"));
    }

    @Test
    public void shouldResendNegotiateAndEstablishOnTimeout() throws IOException
    {
        launch(true);

        final ILink3SessionConfiguration sessionConfiguration = sessionConfiguration();

        connectToTestServer(sessionConfiguration);

        readNegotiate();
        readNegotiate();

        testServer.writeNegotiateResponse();

        readEstablish();
        readEstablish();
        testServer.writeEstablishmentAck();

        testSystem.awaitCompletedReplies(reply);
        session = reply.resultIfPresent();
        assertNotNull(session);
    }

    @Test
    public void shouldDisconnectIfNegotiateNotRespondedTo() throws IOException
    {
        launch(true);

        final ILink3SessionConfiguration sessionConfiguration = sessionConfiguration();

        connectToTestServer(sessionConfiguration);

        readNegotiate();
        readNegotiate();
        assertConnectError(containsString(""));
        assertDisconnected();
    }

    @Test
    public void shouldSupportNegotiationReject() throws IOException
    {
        launch(true);

        final ILink3SessionConfiguration sessionConfiguration = sessionConfiguration();

        connectToTestServer(sessionConfiguration);

        readNegotiate();

        testServer.writeNegotiateReject();

        assertConnectError(containsString("Negotiate rejected"));
        assertDisconnected();
    }

    @Test
    public void shouldSupportEstablishmentReject() throws IOException
    {
        launch(true);

        final ILink3SessionConfiguration sessionConfiguration = sessionConfiguration();
        connectToTestServer(sessionConfiguration);

        readNegotiate();

        testServer.writeEstablishmentReject();

        assertConnectError(containsString("Establishment rejected"));
        assertDisconnected();
    }

    @Test
    public void shouldSupportReestablishingConnections() throws IOException
    {
        shouldExchangeBusinessMessage();

        final long lastUuid = session.uuid();

        final ILink3SessionConfiguration sessionConfiguration = sessionConfiguration()
            .reestablishLastSession(true);
        connectToTestServer(sessionConfiguration);

        testServer.expectedUuid(lastUuid);

        readEstablish(2);
        testServer.writeEstablishmentAck();
    }

    @Test
    public void shouldSupportSequenceMessageHeartbeating() throws IOException
    {
        shouldEstablishConnectionAtBeginningOfWeek();

        // From customer - as a heartbeat message to be sent when a KeepAliveInterval interval from customer lapses
        // and no other message is sent to CME
        sleepHalfInterval();
        testServer.writeSequence(1, KeepAliveLapsed.NotLapsed);
        testServer.readSequence(1, KeepAliveLapsed.NotLapsed);

        // From CME - as a heartbeat message to be sent when a KeepAliveInterval interval from CME lapses and
        // no other message is sent to customer
        final InternalILink3Session session = (InternalILink3Session)this.session;
        final long oldTimeout = session.nextReceiveMessageTimeInMs();
        testServer.writeSequence(1, KeepAliveLapsed.NotLapsed);

        assertEventuallyTrue("Timeout error", () ->
        {
            testSystem.poll();

            final long timeout = session.nextReceiveMessageTimeInMs();

            return timeout > oldTimeout && timeout > System.currentTimeMillis();
        });

        // From CME - when a KeepAliveInterval of the customer lapses without having received any message from them then
        // send message with KeepAliveIntervalLapsed=1 as a warning before initiating disconnect of socket connection
        // Interpret this as a must-reply to these messages
        final long timeout = session.nextSendMessageTimeInMs();
        testServer.writeSequence(1, KeepAliveLapsed.Lapsed);
        testServer.readSequence(1, KeepAliveLapsed.NotLapsed);
        assertThat(System.currentTimeMillis(), lessThan(timeout));

        // From customer - when a KeepAliveInterval of CME lapses without having received any message from CME then send
        // message with KeepAliveIntervalLapsed=1 as a warning before initiating disconnect of socket connection
        // Send a message in order to suppress our own NotLapsed sequence keepalive and force a Lapsed one.
        sleepHalfInterval();
        sendNewOrderSingle();
        testServer.readNewOrderSingle(1);
        testServer.readSequence(2, KeepAliveLapsed.Lapsed);
        testServer.readTerminate();
        testServer.assertDisconnected();
    }

    @Test
    public void shouldSupportNotAppliedMessageSequenceMessageResponse() throws IOException
    {
        // From customer - to reset sequence number in response to Not Applied message sent by CME when CME detects a
        // sequence gap from customer
        shouldEstablishConnectionAtBeginningOfWeek();

        session.nextSentSeqNo(3);
        sendNewOrderSingle();

        testServer.readNewOrderSingle(3);
        testServer.writeNotApplied(1, 2);

        assertEventuallyTrue("", () ->
        {
            testSystem.poll();
            return handler.hasReceivedNotApplied();
        });

        testServer.readSequence(4, KeepAliveLapsed.NotLapsed);
    }

    @Test
    public void shouldSupportRetransmitInResponseToNotAppliedMessage() throws IOException
    {
        handler = new FakeILink3SessionHandler(response ->
        {
            response.retransmit();

            // We shouldn't be allowed to send messages whilst a retransmit is occurring.
            assertThrows(IllegalStateException.class, this::sendNewOrderSingle);
        });

        shouldEstablishConnectionAtBeginningOfWeek();

        sendNewOrderSingle();
        testServer.readNewOrderSingle(1);

        sendNewOrderSingle();
        testServer.readNewOrderSingle(2);

        sendNewOrderSingle();
        testServer.readNewOrderSingle(3);

        // Let's pretend we haven't received 1 and 2 and initiate a resend.
        testServer.writeNotApplied(1, 2);

        testServer.readNewOrderSingle(1);
        testServer.readNewOrderSingle(2);

        assertEventuallyTrue("Session never re-establishes", () ->
        {
            testSystem.poll();
            return session.state() == ILink3Session.State.ESTABLISHED;
        });

        sendNewOrderSingle();
    }

    @Test
    public void shouldRequestRetransmitForSequenceNumberGap() throws IOException
    {
        shouldEstablishConnectionAtBeginningOfWeek();

        sendNewOrderSingle();
        testServer.readNewOrderSingle(1);

        testServer.writeExecutionReportStatus(3, false);

        testServer.acceptRetransRequest(1, 2);

        testServer.writeExecutionReportStatus(1, true);
        testServer.writeExecutionReportStatus(4, false);
        testServer.writeExecutionReportStatus(2, true);

        agreeRecvSeqNo(5);

        terminateAndDisconnect();
    }

    @Test
    public void shouldOnlyHaveASingleRequestRetransmitInflight() throws IOException
    {
        shouldEstablishConnectionAtBeginningOfWeek();

        testServer.writeExecutionReportStatus(2, false);
        testServer.writeExecutionReportStatus(4, false);

        testServer.acceptRetransRequest(1, 1);
        agreeRecvSeqNo(5);

        // Ensure that the second retransmit request isn't sent yet
        sendNewOrderSingle();
        testServer.readNewOrderSingle(1);

        // Fill First
        assertEquals(1, session.retransmitFillSeqNo());
        testServer.writeExecutionReportStatus(1, true);

        testServer.acceptRetransRequest(3, 1);

        // Fill second
        assertEquals(3, session.retransmitFillSeqNo());
        testServer.writeExecutionReportStatus(3, true);

        agreeRetransmitFillSeqNo(NOT_AWAITING_RETRANSMIT);
        agreeRecvSeqNo(5);

        terminateAndDisconnect();
    }

    @Test
    public void shouldLimitLargeRetransmitRequestsIntoBatches() throws IOException
    {
        shouldEstablishConnectionAtBeginningOfWeek();

        testServer.writeExecutionReportStatus(5000, false);

        testServer.acceptRetransRequest(1, 2500);
        writeExecutionReports(1, 2500);

        testServer.canSkip(Sequence506Decoder.TEMPLATE_ID);

        testServer.acceptRetransRequest(2501, 2499);
        writeExecutionReports(2501, 2499);

        agreeRetransmitFillSeqNo(NOT_AWAITING_RETRANSMIT);
        agreeRecvSeqNo(5001);

        terminateAndDisconnect();
    }

    @Test
    public void shouldNotStallUponARetransmitReject() throws IOException
    {
        shouldEstablishConnectionAtBeginningOfWeek();

        testServer.writeExecutionReportStatus(5000, false);

        testServer.rejectRetransRequest(1, 2500);
        testServer.rejectRetransRequest(2501, 2499);

        agreeRetransmitFillSeqNo(NOT_AWAITING_RETRANSMIT);
        agreeRecvSeqNo(5001);

        verify(handler, times(2))
            .onRetransmitReject(eq(RETRANSMIT_REJECT_REASON), anyLong(), eq(RETRANSMIT_REJECT_ERROR_CODES));

        terminateAndDisconnect();
    }

    private void writeExecutionReports(final int fromSeqNo, final int msgCount)
    {
        final int lastSeqNo = fromSeqNo + msgCount;
        for (int i = fromSeqNo; i < lastSeqNo; i++)
        {
            testServer.writeExecutionReportStatus(i, true);
        }
    }

    private void agreeRecvSeqNo(final long nextRecvSeqNo)
    {
        agreeEquals(session::nextRecvSeqNo, nextRecvSeqNo);
    }

    private void agreeRetransmitFillSeqNo(final long retransmitFillSeqNo)
    {
        agreeEquals(session::retransmitFillSeqNo, retransmitFillSeqNo);
    }

    private void agreeEquals(final LongSupplier supplier, final long value)
    {
        assertEventuallyTrue("Fails to agree value " + value, () ->
        {
            testSystem.poll();
            return supplier.getAsLong() == value;
        });
    }

    private void sleepHalfInterval()
    {
        testSystem.awaitBlocking(() -> sleep(TEST_KEEP_ALIVE_INTERVAL_IN_MS / 2));
    }

    private void sleep(final int timeInMs)
    {
        try
        {
            Thread.sleep(timeInMs);
        }
        catch (final InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private void connectToTestServer(final ILink3SessionConfiguration sessionConfiguration) throws IOException
    {
        testServer = new ILink3TestServer(port, () -> reply = library.initiate(sessionConfiguration), testSystem);
    }

    private void assertConnectError(final Matcher<String> messageMatcher)
    {
        testSystem.awaitReply(reply);
        assertEquals(Reply.State.ERRORED, reply.state());
        assertThat(reply.error().getMessage(), messageMatcher);
    }

    private void readEstablish()
    {
        readEstablish(1L);
    }

    private void readEstablish(final long expectedNextSeqNo)
    {
        testServer.readEstablish(ACCESS_KEY_ID, FIRM_ID, SESSION_ID, TEST_KEEP_ALIVE_INTERVAL_IN_MS, expectedNextSeqNo);
    }

    private void readNegotiate()
    {
        testServer.readNegotiate(ACCESS_KEY_ID, FIRM_ID);
    }

    private void startTerminate()
    {
        testSystem.awaitSend(
            "Failed to send terminate", () -> session.terminate("shutdown", 0));
    }

    private ILink3SessionConfiguration sessionConfiguration()
    {
        return new ILink3SessionConfiguration()
            .host("localhost")
            .port(port)
            .sessionId(SESSION_ID)
            .firmId(FIRM_ID)
            .userKey(USER_KEY)
            .accessKeyId(ACCESS_KEY_ID)
            .requestedKeepAliveIntervalInMs(TEST_KEEP_ALIVE_INTERVAL_IN_MS)
            .handler(handler);
    }

    private void assertDisconnected()
    {
        testServer.assertDisconnected();
        assertThat(library.iLink3Sessions(), hasSize(0));
    }

    private void sendNewOrderSingle()
    {
        final NewOrderSingle514Encoder newOrderSingle = new NewOrderSingle514Encoder();
        assertThat(session.claimMessage(newOrderSingle), greaterThan(0L));
        newOrderSingle
            .partyDetailsListReqID(1)
            .orderQty(1)
            .senderID(FIRM_ID)
            .side(SideReq.Buy)
            .clOrdID(CL_ORD_ID)
            .partyDetailsListReqID(1)
            .orderRequestID(1);

        session.commit();
    }

    private void terminateAndDisconnect()
    {
        startTerminate();

        testServer.readTerminate();
        testServer.writeTerminate();

        testSystem.awaitUnbind(session);

        assertDisconnected();
    }
}
