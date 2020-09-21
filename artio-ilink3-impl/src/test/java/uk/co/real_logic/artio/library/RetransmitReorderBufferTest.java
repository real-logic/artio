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
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.ilink.ILink3Proxy;
import uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader;
import uk.co.real_logic.artio.protocol.GatewayPublication;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader.SOFH_LENGTH;
import static uk.co.real_logic.artio.library.ILink3Connection.NOT_AWAITING_RETRANSMIT;

public class RetransmitReorderBufferTest
{
    private static final int LIBRARY_ID = 2;
    private static final long CONNECTION_ID = 3;
    private static final long UUID = 4;
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
    private long nanoTime = System.nanoTime();
    private final EpochNanoClock clock = () -> nanoTime;
    private InternalILink3Connection connection;

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
            null,
            outboundPublication,
            inboundPublication,
            LIBRARY_ID,
            null,
            UUID,
            LAST_RECEIVED_SEQ_NO,
            LAST_SENT_SEQ_NO,
            false,
            UUID,
            clock,
            proxy);

        verify(proxy).businessMessageLogger();

        connection.state(ILink3Connection.State.ESTABLISHED);

        onExecutionReport(5, false);

        // We're ready for a retransmit in all cases
        assertSeqNos(6, 4);

        verifyRetransmitRequest(2L, 3, -1);
    }

    private void verifyRetransmitRequest(final long fromSeqNo, final int msgCount)
    {
        verifyRetransmitRequest(fromSeqNo, msgCount, UUID);
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
        SimpleOpenFramingHeader.writeSofh(recvBuffer, 0, totalLength);

        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final ExecutionReportStatus532Encoder executionReportStatus = new ExecutionReportStatus532Encoder();
        executionReportStatus.wrapAndApplyHeader(recvBuffer, SOFH_LENGTH, headerEncoder);

        executionReportStatus
            .seqNum(sequenceNumber)
            .uUID(UUID)
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

        final long position = connection.onMessage(
            recvBuffer,
            SOFH_LENGTH + MessageHeaderEncoder.ENCODED_LENGTH,
            ExecutionReportStatus532Encoder.TEMPLATE_ID,
            ExecutionReportStatus532Encoder.BLOCK_LENGTH,
            ExecutionReportStatus532Encoder.SCHEMA_VERSION,
            totalLength);

        assertThat(position, Matchers.greaterThanOrEqualTo(0L));
    }

    @After
    public void done()
    {
        assertEquals(0, connection.retransmitQueueSize());
        verifyNoMoreInteractions(proxy);
    }

    @Test
    public void shouldBufferWhenReceivingOutOfOrder()
    {
        // @5,2R,3R,4R,done.
        onExecutionReport(2, true);
        onExecutionReport(3, true);
        onExecutionReport(4, true);
        onExecutionReport(6, false);

        assertSeqNos(7, NOT_AWAITING_RETRANSMIT);
        assertThat(handler.sequenceNumbers(), contains(2L, 3L, 4L, 5L, 6L));
    }

    @Test
    public void shouldAcceptInterleaving()
    {
        // @5,2R,6,3R,7,4R,done.
        onExecutionReport(2, true);
        onExecutionReport(6, false);
        onExecutionReport(3, true);
        onExecutionReport(7, false);
        onExecutionReport(4, true);

        assertSeqNos(8, NOT_AWAITING_RETRANSMIT);
        assertThat(handler.sequenceNumbers(), contains(2L, 3L, 4L, 5L, 6L, 7L));
    }

    @Test
    public void shouldNotifyAndQueueReRequestWhenMaxSizeBreached()
    {
        // buffer has size for 3 messages, this sends 4
        // @5,2R,6,3R,7,8,4R,done.
        onExecutionReport(2, true);
        onExecutionReport(6, false);
        onExecutionReport(3, true);
        onExecutionReport(7, false);
        onExecutionReport(8, false);
        onExecutionReport(4, true);

        assertSeqNos(9, 8);
        assertThat(handler.sequenceNumbers(), contains(2L, 3L, 4L, 5L, 6L, 7L));
        handler.sequenceNumbers().clear();

        verifyRetransmitRequest(8L, 1);

        // fill the second retransmit request
        onExecutionReport(8, true);
        assertThat(handler.sequenceNumbers(), contains(8L));
        assertSeqNos(9, NOT_AWAITING_RETRANSMIT);
    }

    @Test
    public void shouldNotifyAndQueueReRequestWhenMaxSizeBreachedMultipleMessges()
    {
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
        assertThat(handler.sequenceNumbers(), contains(2L, 3L, 4L, 5L, 6L, 7L));
        handler.sequenceNumbers().clear();

        verifyRetransmitRequest(8L, 2);

        // fill the second retransmit request
        onExecutionReport(8, true);
        onExecutionReport(9, true);
        assertThat(handler.sequenceNumbers(), contains(8L, 9L));
        assertSeqNos(10, NOT_AWAITING_RETRANSMIT);
    }

    // shouldNotifyWhenTimeoutBreached()

    // TODO: gaps within the retransmit
    // TODO: gaps within the normal message sequence
    // TODO: shouldNotifyAndQueueReRequestWhenMaxSizeBreachedMultipleMessges with a gap in the retransmit if possible

    // TODO: also integrate interleaving case into system test
}
