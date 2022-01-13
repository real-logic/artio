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
package uk.co.real_logic.artio.library;

import iLinkBinary.*;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.artio.fixp.FixPMessageDissector;
import uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader;
import uk.co.real_logic.artio.ilink.ILink3Connection;
import uk.co.real_logic.artio.ilink.ILink3ConnectionConfiguration;
import uk.co.real_logic.artio.ilink.ILink3Proxy;
import uk.co.real_logic.artio.protocol.GatewayPublication;

import java.util.stream.LongStream;

import static iLinkBinary.RetransmitRequest508Decoder.lastUUIDNullValue;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.SOFH_LENGTH;
import static uk.co.real_logic.artio.ilink.ILink3Connection.NOT_AWAITING_RETRANSMIT;
import static uk.co.real_logic.artio.ilink.ILink3ConnectionConfiguration.DEFAULT_RETRANSMIT_TIMEOUT_IN_MS;

public class RetransmitQueueTest
{
    private static final int LIBRARY_ID = 2;
    private static final long CONNECTION_ID = 3;
    private static final long LAST_UUID = 4;
    private static final long UUID = 5;
    private static final long LAST_RECEIVED_SEQ_NO = 1;
    private static final long LAST_SENT_SEQ_NO = 1;

    private static final int MAX_RETRANSMIT_QUEUE_SIZE = 1676;
    private static final String ACCESS_KEY_ID = "12345678901234567890";
    private static final String SESSION_ID = "ABC";
    private static final String FIRM_ID = "DEFGH";
    private static final String USER_KEY = "somethingprivate";
    private static final String CL_ORD_ID = "123";

    // @1,Received 5
    private final int totalLength = SOFH_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH +
        ExecutionReportStatus532Encoder.BLOCK_LENGTH;
    private final UnsafeBuffer recvBuffer = new UnsafeBuffer(new byte[totalLength]);
    private final GatewayPublication outboundPublication = mock(GatewayPublication.class);
    private final GatewayPublication inboundPublication = mock(GatewayPublication.class);
    private final SequenceNumberCheckingHandler handler = new SequenceNumberCheckingHandler();
    private final ILink3Proxy proxy = mock(ILink3Proxy.class);
    private long nanoTime = 10;
    private final EpochNanoClock clock = () -> nanoTime;
    private InternalILink3Connection connection;
    private int expectedRetransmitQueueSize = 0;

    @Before
    public void setUp()
    {
        final ILink3ConnectionConfiguration config = new ILink3ConnectionConfiguration.Builder()
            .host("localhost")
            .handler(handler)
            .firmId(FIRM_ID)
            .userKey(USER_KEY)
            .accessKeyId(ACCESS_KEY_ID)
            .sessionId(SESSION_ID)
            .port(123)
            .reEstablishLastConnection(true)
            .maxRetransmitQueueSizeInBytes(MAX_RETRANSMIT_QUEUE_SIZE)
            .build();

        connection = new InternalILink3Connection(
            config,
            CONNECTION_ID,
            mock(InitiateILink3ConnectionReply.class),
            outboundPublication,
            inboundPublication,
            LIBRARY_ID,
            null,
            UUID,
            LAST_RECEIVED_SEQ_NO,
            LAST_SENT_SEQ_NO,
            false,
            LAST_UUID,
            clock,
            proxy,
            mock(FixPMessageDissector.class));
    }

    @After
    public void done()
    {
        assertEquals(expectedRetransmitQueueSize, connection.retransmitQueueSize());
        verifyNoMoreInteractions(proxy);

        connection.poll(DEFAULT_RETRANSMIT_TIMEOUT_IN_MS + 1);
        assertFalse(handler.retransmitTimedOut());
    }

    @Test
    public void shouldQueueWhenReceivingOutOfOrder()
    {
        setupRetransmit();

        // @5,2R,3R,4R,done.
        onExecutionReport(2, true);
        onExecutionReport(3, true);
        onExecutionReport(4, true);
        onExecutionReport(6, false);

        assertSeqNos(7, NOT_AWAITING_RETRANSMIT);
        assertSequenceNumbers(contains(2L, 3L, 4L, 5L, 6L));
    }

    @Test
    public void shouldAcceptInterleaving()
    {
        setupRetransmit();

        // @5,2R,6,3R,7,4R,done.
        onExecutionReport(2, true);
        onExecutionReport(6, false);
        onExecutionReport(3, true);
        onExecutionReport(7, false);
        onExecutionReport(4, true);

        assertSeqNos(8, NOT_AWAITING_RETRANSMIT);
        assertSequenceNumbers(contains(2L, 3L, 4L, 5L, 6L, 7L));
    }

    @Test
    public void shouldNotifyAndQueueReRequestWhenMaxSizeBreached()
    {
        setupRetransmit();

        // buffer has size for 3 messages, this sends 4
        // @5,2R,6,3R,7,8,4R,done.
        onExecutionReport(2, true);
        onExecutionReport(6, false);
        onExecutionReport(3, true);
        onExecutionReport(7, false);
        onExecutionReport(8, false);
        onExecutionReport(4, true);

        assertSeqNos(9, 8);
        assertSequenceNumbers(contains(2L, 3L, 4L, 5L, 6L, 7L));
        clearSequenceNumbers();

        verifyRetransmitRequest(8L, 1);

        // fill the second retransmit request
        onExecutionReport(8, true);
        assertSequenceNumbers(contains(8L));
        assertSeqNos(9, NOT_AWAITING_RETRANSMIT);
    }

    private void clearSequenceNumbers()
    {
        handler.sequenceNumbers().clear();
    }

    @Test
    public void shouldNotifyAndQueueReRequestWhenMaxSizeBreachedMultipleMessges()
    {
        setupRetransmit();

        // buffer has size for 3 messages, this sends 4
        // @5,2R,6,3R,7,8,9,4R,done.
        onExecutionReport(2, true);
        onExecutionReport(6, false);
        onExecutionReport(3, true);
        onExecutionReport(7, false);
        onExecutionReport(8, false);
        onExecutionReport(9, false);
        onExecutionReport(4, true);

        assertSeqNos(10, 9);
        assertSequenceNumbers(contains(2L, 3L, 4L, 5L, 6L, 7L));
        clearSequenceNumbers();

        verifyRetransmitRequest(8L, 2);

        // fill the second retransmit request
        onExecutionReport(8, true);
        onExecutionReport(9, true);
        assertSequenceNumbers(contains(8L, 9L));
        assertSeqNos(10, NOT_AWAITING_RETRANSMIT);
    }

    @Test
    public void shouldSupportQueueOrderingForGapsOver2500MessagesInLength()
    {
        // Setup large retransmit gaps
        givenEstablished();
        onExecutionReport(2510, false);
        assertSeqNos(2511, 2501);
        verifyRetransmitRequest(2L, 2500);
        reset(proxy);

        onExecutionReports(2, 1000);
        onExecutionReport(2511, false);
        onExecutionReports(1001, 2501);

        assertSequenceNumbers(containsRange(2, 2501));
        clearSequenceNumbers();
        assertSeqNos(2512, 2509);
        verifyRetransmitRequest(2502L, 8);

        // fill the second retransmit request
        onExecutionReports(2502, 2509);
        assertSequenceNumbers(containsRange(2502, 2511));
        assertSeqNos(2512, NOT_AWAITING_RETRANSMIT);
    }

    @Ignore
    @Test
    public void shouldNotifyWhenTimeoutBreached()
    {
        setupRetransmit();

        expectedRetransmitQueueSize = 492;

        assertFalse("Wrong retransmitTimedOut", handler.retransmitTimedOut());
        nanoTime += MILLISECONDS.toNanos(DEFAULT_RETRANSMIT_TIMEOUT_IN_MS) + 1;
        connection.poll(NANOSECONDS.toMillis(nanoTime));
        assertTrue("Wrong retransmitTimedOut", handler.retransmitTimedOut());
        handler.resetRetransmitTimedOut();

        connection.poll(NANOSECONDS.toMillis(nanoTime));
        assertFalse("retransmitTimedOut called again unnecessarily", handler.retransmitTimedOut());

        handler.resetRetransmitTimedOut();
    }

    @Test
    public void shouldReplayQueueWhenReceivingSequenceMessage()
    {
        setupRetransmit();

        // @5,6,Seq8,done.
        onExecutionReport(6, false);
        connection.onSequence(connection.uuid(), 7, FTI.Primary, KeepAliveLapsed.NotLapsed);

        assertSeqNos(7, NOT_AWAITING_RETRANSMIT);
        assertSequenceNumbers(contains(5L, 6L));
    }

    @Test
    public void shouldQueueRetransmitForRetransmitGapWithinRetransmit()
    {
        setupRetransmit();

        // @5,2R,3R,4R,done.
        onExecutionReport(2, true);
        onExecutionReport(4, true);
        onExecutionReport(6, false);

        assertSeqNos(7, 3);
        assertSequenceNumbers(contains(2L));
        clearSequenceNumbers();

        verifyRetransmitRequest(3, 1);
        onExecutionReport(3, true);
        assertSequenceNumbers(contains(3L, 4L, 5L, 6L));
        assertSeqNos(7, NOT_AWAITING_RETRANSMIT);
    }

    @Test
    public void shouldQueueRetransmitForNormalGapWithinRetransmit()
    {
        setupRetransmit();

        // @5,7,2R,3R,4R,6,done.
        onExecutionReport(7, false);
        onExecutionReport(2, true);
        onExecutionReport(3, true);
        onExecutionReport(4, true);

        assertSeqNos(8, 6);
        assertSequenceNumbers(contains(2L, 3L, 4L, 5L));
        clearSequenceNumbers();

        verifyRetransmitRequest(6, 1);
        onExecutionReport(6, true);
        assertSequenceNumbers(contains(6L, 7L));
        assertSeqNos(8, NOT_AWAITING_RETRANSMIT);
    }

    @Test
    public void shouldQueueWhenReceivingLastUuidRetransmit()
    {
        setupLastUuidRetransmit();

        onExecutionReport(2, false);
        onExecutionReport(2, true, LAST_UUID);
        onExecutionReport(3, false);
        onExecutionReport(3, true, LAST_UUID);

        assertSeqNos(4, NOT_AWAITING_RETRANSMIT);
        assertSequenceNumbers(contains(2L, 3L, 2L, 3L));
        assertThat(handler.uuids(), contains(LAST_UUID, LAST_UUID, UUID, UUID));
    }

    @Test
    public void shouldReplayQueueWhenReceivingLastUuidSequence()
    {
        setupLastUuidRetransmit();

        onExecutionReport(2, false);
        connection.onSequence(UUID, 3, FTI.Primary, KeepAliveLapsed.NotLapsed);

        assertSeqNos(3, NOT_AWAITING_RETRANSMIT);
        assertSequenceNumbers(contains(2L));
        assertThat(handler.uuids(), contains(UUID));
    }

    @Test
    public void shouldQueueRetransmitWithinLastUuidRetransmit()
    {
        setupLastUuidRetransmit();

        onExecutionReport(2, false);
        onExecutionReport(3, false);
        onExecutionReport(3, true, LAST_UUID);

        assertSeqNos(4, 2);
        assertReceivedNoSequenceNumbers();
        assertThat(handler.uuids(), hasSize(0));
        verifyRetransmitRequest(2, 1, LAST_UUID);

        onExecutionReport(2, true, LAST_UUID);

        assertSeqNos(4, NOT_AWAITING_RETRANSMIT);
        assertSequenceNumbers(contains(2L, 3L, 2L, 3L));
        assertThat(handler.uuids(), contains(LAST_UUID, LAST_UUID, UUID, UUID));
    }

    @Test
    public void shouldNotifyAndQueueReRequestWhenMaxSizeMultipleMessgesBreachedLastUuidRetransmit()
    {
        setupLastUuidRetransmit();

        // buffer has size for 3 messages, this sends 5
        // RR 4,2-2, @1
        onExecutionReport(2, false);
        onExecutionReport(2, true, LAST_UUID);
        onExecutionReport(3, false);
        onExecutionReport(4, false);
        onExecutionReport(5, false);
        onExecutionReport(6, false);
        onExecutionReport(3, true, LAST_UUID);

        assertSeqNos(7, 6);
        assertSequenceNumbers(contains(2L, 3L, 2L, 3L, 4L));
        assertThat(handler.uuids(), contains(LAST_UUID, LAST_UUID, UUID, UUID, UUID));
        verifyRetransmitRequest(5, 2);
        clearSequenceNumbers();
        handler.uuids().clear();

        // fill the second retransmit request
        onExecutionReport(5, true);
        onExecutionReport(6, true);
        assertSequenceNumbers(contains(5L, 6L));
        assertThat(handler.uuids(), contains(UUID, UUID));
        assertSeqNos(7, NOT_AWAITING_RETRANSMIT);
    }

    @Test
    public void shouldNotHandoffNonContiguousReplayedMessagesAfterSequenceGapInRetransmit()
    {
        givenEstablished();
        connection.nextRecvSeqNo(2566);

        onExecutionReport(2572, false);
        assertSeqNos(2573, 2571);
        verifyRetransmitRequest(2566, 6);
        reset(proxy);
        assertReceivedNoSequenceNumbers();

        onExecutionReport(2566, true);
        onExecutionReport(2567, true);
        assertSequenceNumbers(contains(2566L, 2567L));
        clearSequenceNumbers();

        // detects next retransmit and queues it
        onExecutionReport(2570, true);
        // deems current retransmit complete
        onExecutionReport(2571, true);
        verifyRetransmitRequest(2568, 2);
        assertSeqNos(2573, 2569);

        onExecutionReport(2568, true);
        onExecutionReport(2569, true);
        assertSequenceNumbers(contains(2568L, 2569L, 2570L, 2571L, 2572L));
        assertSeqNos(2573, NOT_AWAITING_RETRANSMIT);
    }

    private void assertSequenceNumbers(final Matcher<Iterable<? extends Long>> matcher)
    {
        assertThat(handler.sequenceNumbers().toString(), handler.sequenceNumbers(), matcher);
    }

    private void setupLastUuidRetransmit()
    {
        connection.state(ILink3Connection.State.SENT_ESTABLISH);

        connection.onEstablishmentAck(UUID, 0, 1, 3, LAST_UUID, 1, 1);

        verifyRetransmitRequest(2, 2, LAST_UUID);
    }

    private void setupRetransmit()
    {
        givenEstablished();

        onExecutionReport(5, false);

        // We're ready for a retransmit in all cases
        assertSeqNos(6, 4);

        verifyRetransmitRequest(2L, 3);

        reset(proxy);
    }

    private void givenEstablished()
    {
        connection.state(ILink3Connection.State.ESTABLISHED);
    }

    private void verifyRetransmitRequest(final long fromSeqNo, final int msgCount)
    {
        verifyRetransmitRequest(fromSeqNo, msgCount, lastUUIDNullValue());
    }

    private void verifyRetransmitRequest(final long fromSeqNo, final int msgCount, final long lastUuid)
    {
        verify(proxy).sendRetransmitRequest(eq(UUID), eq(lastUuid), anyLong(), eq(fromSeqNo), eq(msgCount));
    }

    private void assertSeqNos(final long nextRecvSeqNo, final long retransmitFillSeqNo)
    {
        assertEquals("wrong nextRecvSeqNo", nextRecvSeqNo, connection.nextRecvSeqNo());
        assertEquals("wrong retransmitFillSeqNo", retransmitFillSeqNo, connection.retransmitFillSeqNo());
    }

    private void onExecutionReport(final long sequenceNumber, final boolean possRetrans)
    {
        onExecutionReport(sequenceNumber, possRetrans, UUID);
    }

    private void onExecutionReport(final long sequenceNumber, final boolean possRetrans, final long uuid)
    {
        SimpleOpenFramingHeader.writeILinkSofh(recvBuffer, 0, totalLength);

        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final ExecutionReportStatus532Encoder executionReportStatus = new ExecutionReportStatus532Encoder();
        executionReportStatus.wrapAndApplyHeader(recvBuffer, SOFH_LENGTH, headerEncoder);

        executionReportStatus
            .seqNum(sequenceNumber)
            .uUID(uuid)
            .text("")
            .execID("123")
            .senderID(FIRM_ID)
            .clOrdID(CL_ORD_ID)
            .partyDetailsListReqID(1)
            .orderID(1)
            .transactTime(nanoTime)
            .sendingTimeEpoch(nanoTime)
            .orderRequestID(1)
            .location("LONDO")
            .securityID(1)
            .orderQty(1)
            .cumQty(1)
            .leavesQty(1)
            .expireDate(1)
            .ordStatus(OrderStatus.Filled)
            .side(SideReq.Buy)
            .timeInForce(TimeInForce.Day)
            .possRetransFlag(possRetrans ? BooleanFlag.True : BooleanFlag.False)
            .shortSaleType(ShortSaleType.LongSell);

        final Action position = connection.onMessage(
            recvBuffer,
            SOFH_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH,
            ExecutionReportStatus532Encoder.TEMPLATE_ID,
            ExecutionReportStatus532Encoder.BLOCK_LENGTH,
            ExecutionReportStatus532Encoder.SCHEMA_VERSION,
            totalLength);

        assertEquals(CONTINUE, position);
    }

    private Matcher<Iterable<? extends Long>> containsRange(final long from, final long toInclusive)
    {
        return contains(LongStream.rangeClosed(from, toInclusive).boxed().toArray(Long[]::new));
    }

    private void onExecutionReports(final int fromSequenceNumber, final int toSequenceNumberInclusive)
    {
        for (int sequenceNumber = fromSequenceNumber; sequenceNumber <= toSequenceNumberInclusive; sequenceNumber++)
        {
            onExecutionReport(sequenceNumber, true);
        }
    }

    private void assertReceivedNoSequenceNumbers()
    {
        assertThat(handler.sequenceNumbers(), hasSize(0));
    }
}
