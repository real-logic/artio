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

import iLinkBinary.*;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageDecoderFlyweight;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.system_tests.TestSystem;
import uk.co.real_logic.artio.util.TimeUtil;
import uk.co.real_logic.sbe.json.JsonPrinter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static iLinkBinary.NegotiationResponse501Encoder.credentialsHeaderLength;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.LogTag.FIX_TEST;
import static uk.co.real_logic.artio.ilink.ILink3Proxy.ILINK_HEADER_LENGTH;
import static uk.co.real_logic.artio.ilink.ILink3SystemTest.CL_ORD_ID;
import static uk.co.real_logic.artio.ilink.ILink3SystemTest.FIRM_ID;
import static uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader.*;

public class ILink3TestServer
{
    private static final int BUFFER_SIZE = 8 * 1024;
    public static final String REJECT_REASON = "Invalid Logon";
    public static final int ESTABLISHMENT_REJECT_SEQ_NO = 2;

    private final JsonPrinter jsonPrinter = new JsonPrinter(ILink3Offsets.loadSbeIr());
    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final UnsafeBuffer unsafeWriteBuffer = new UnsafeBuffer(writeBuffer);
    private final ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final UnsafeBuffer unsafeReadBuffer = new UnsafeBuffer(readBuffer);

    private final MessageHeaderDecoder iLinkHeaderDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder iLinkHeaderEncoder = new MessageHeaderEncoder();

    private final SocketChannel socket;
    private final TestSystem testSystem;

    private long uuid;
    private long establishRequestTimestamp;
    private long negotiateRequestTimestamp;
    private int requestedKeepAliveInterval;

    public ILink3TestServer(
        final int port,
        final Runnable connectOperation,
        final TestSystem testSystem) throws IOException
    {
        this.testSystem = testSystem;
        try (ServerSocketChannel server = ServerSocketChannel
            .open()
            .bind(new InetSocketAddress("localhost", port)))
        {
            server.configureBlocking(false);

            connectOperation.run();

            SocketChannel socket;
            while ((socket = server.accept()) == null)
            {
                testSystem.poll();
                Thread.yield();
            }

            this.socket = socket;
        }

        iLinkHeaderDecoder.wrap(unsafeReadBuffer, SOFH_LENGTH);
        iLinkHeaderEncoder.wrap(unsafeWriteBuffer, SOFH_LENGTH);
    }

    public <T extends MessageDecoderFlyweight> T read(final T messageDecoder, final int nonBlockLength)
    {
        return testSystem.awaitBlocking(() ->
        {
            try
            {
                final int messageOffset = iLinkHeaderDecoder.encodedLength() + SOFH_LENGTH;
                final int expectedLength = messageOffset + messageDecoder.sbeBlockLength() + nonBlockLength;
                readBuffer.limit(expectedLength);

                final int read = socket.read(readBuffer);
                final int totalLength = readSofh(unsafeReadBuffer, 0);

                final int blockLength = iLinkHeaderDecoder.blockLength();
                messageDecoder.wrap(
                    unsafeReadBuffer,
                    messageOffset,
                    blockLength,
                    iLinkHeaderDecoder.version());

                print(unsafeReadBuffer, "> ");

                assertEquals(messageDecoder.sbeTemplateId(), iLinkHeaderDecoder.templateId());

                if (totalLength != read)
                {
                    throw new IllegalArgumentException("totalLength=" + totalLength + ",read=" + read);
                }

                assertThat(read, greaterThanOrEqualTo(messageOffset + blockLength));

                readBuffer.clear();

                return messageDecoder;
            }
            catch (final IOException e)
            {
                LangUtil.rethrowUnchecked(e);
                return null;
            }
        });
    }

    private void print(final UnsafeBuffer unsafeReadBuffer, final String prefixString)
    {
        if (DebugLogger.isEnabled(FIX_TEST))
        {
            final StringBuilder sb = new StringBuilder();
            jsonPrinter.print(sb, unsafeReadBuffer, SOFH_LENGTH);
            DebugLogger.log(FIX_TEST, prefixString, sb.toString());
        }
    }

    private void wrap(final MessageEncoderFlyweight messageEncoder, final int length)
    {
        final int messageSize = ILINK_HEADER_LENGTH + length;
        writeSofh(unsafeWriteBuffer, 0, messageSize);
        DebugLogger.log(FIX_TEST, "wrap messageSize=", String.valueOf(messageSize));

        iLinkHeaderEncoder
            .wrap(unsafeWriteBuffer, SOFH_LENGTH)
            .blockLength(messageEncoder.sbeBlockLength())
            .templateId(messageEncoder.sbeTemplateId())
            .schemaId(messageEncoder.sbeSchemaId())
            .version(messageEncoder.sbeSchemaVersion());

        messageEncoder.wrap(unsafeWriteBuffer, ILINK_HEADER_LENGTH);
    }

    private void write()
    {
        final int messageSize = readSofhMessageSize(unsafeWriteBuffer, 0);
        writeBuffer.position(0).limit(messageSize);

        testSystem.awaitBlocking(() ->
        {
            try
            {
                print(unsafeWriteBuffer, "< ");

                final int written = socket.write(writeBuffer);
                assertEquals(messageSize, written);
            }
            catch (final IOException e)
            {
                e.printStackTrace();
            }
            finally
            {
                writeBuffer.clear();
            }
        });
    }

    public void readNegotiate(final String expectedAccessKeyId, final String expectedFirmId)
    {
        final Negotiate500Decoder negotiate = read(
            new Negotiate500Decoder(), Negotiate500Decoder.credentialsHeaderLength());
        assertEquals(expectedAccessKeyId, negotiate.accessKeyID());

        assertEquals(expectedFirmId, negotiate.firm());
        assertEquals(0, negotiate.credentialsLength());

        uuid = negotiate.uUID();
        negotiateRequestTimestamp = negotiate.requestTimestamp();
    }

    public long uuid()
    {
        return uuid;
    }

    public void writeNegotiateResponse()
    {
        final NegotiationResponse501Encoder negotiateResponse = new NegotiationResponse501Encoder();
        wrap(negotiateResponse, NegotiationResponse501Encoder.BLOCK_LENGTH + credentialsHeaderLength());

        negotiateResponse
            .uUID(uuid)
            .requestTimestamp(negotiateRequestTimestamp)
            .secretKeySecureIDExpiration(1)
            .faultToleranceIndicator(FTI.Primary)
            .splitMsg(SplitMsg.NULL_VAL)
            .previousSeqNo(0)
            .previousUUID(0);

        write();
    }

    public void writeNegotiateReject()
    {
        final NegotiationReject502Encoder negotiateReject = new NegotiationReject502Encoder();
        wrap(negotiateReject, NegotiationReject502Encoder.BLOCK_LENGTH);

        negotiateReject
            .uUID(uuid)
            .reason(REJECT_REASON)
            .requestTimestamp(negotiateRequestTimestamp)
            .errorCodes(0)
            .faultToleranceIndicator(FTI.Primary)
            .splitMsg(SplitMsg.NULL_VAL);

        write();
    }

    public void readEstablish(
        final String expectedAccessKeyID, final String expectedFirmId, final String expectedSessionId,
        final int expectedKeepAliveInterval, final long expectedNextSeqNo)
    {
        final Establish503Decoder establish = read(
            new Establish503Decoder(), Establish503Decoder.credentialsHeaderLength());
        //  establish.hMACSignature()
        assertEquals(expectedAccessKeyID, establish.accessKeyID());
        // TradingSystemInfo

        final long uuid = establish.uUID();
        assertEquals(this.uuid, uuid);

        establishRequestTimestamp = establish.requestTimestamp();
        assertThat(establishRequestTimestamp, greaterThanOrEqualTo(negotiateRequestTimestamp));
        final long nextSeqNo = establish.nextSeqNo();
        assertEquals(expectedNextSeqNo, nextSeqNo);

        assertEquals(expectedSessionId, establish.session());
        assertEquals(expectedFirmId, establish.firm());
        requestedKeepAliveInterval = establish.keepAliveInterval();
        assertEquals(expectedKeepAliveInterval, requestedKeepAliveInterval);
    }

    public void writeEstablishmentAck()
    {
        final EstablishmentAck504Encoder establishmentAck = new EstablishmentAck504Encoder();
        wrap(establishmentAck, EstablishmentAck504Encoder.BLOCK_LENGTH);

        establishmentAck
            .uUID(uuid)
            .requestTimestamp(establishRequestTimestamp)
            .nextSeqNo(1)
            .previousSeqNo(0)
            .previousUUID(0)
            .keepAliveInterval(requestedKeepAliveInterval + 100)
            .secretKeySecureIDExpiration(1)
            .faultToleranceIndicator(FTI.Primary)
            .splitMsg(SplitMsg.NULL_VAL);

        write();
    }

    public void writeEstablishmentReject()
    {
        final EstablishmentReject505Encoder establishmentReject = new EstablishmentReject505Encoder();
        wrap(establishmentReject, EstablishmentReject505Encoder.BLOCK_LENGTH);

        establishmentReject
            .reason(REJECT_REASON)
            .uUID(uuid)
            .requestTimestamp(establishRequestTimestamp)
            .nextSeqNo(ESTABLISHMENT_REJECT_SEQ_NO)
            .errorCodes(1)
            .faultToleranceIndicator(FTI.Primary)
            .splitMsg(SplitMsg.NULL_VAL);

        write();
    }

    public void readTerminate()
    {
        final Terminate507Decoder terminate = read(new Terminate507Decoder(), 0);
//        terminate.reason();
        assertEquals(uuid, terminate.uUID());
//        terminate.requestTimestamp();
//        terminate.errorCodes();
    }

    public void writeTerminate()
    {
        final Terminate507Encoder terminate = new Terminate507Encoder();
        wrap(terminate, Terminate507Encoder.BLOCK_LENGTH);

        terminate
            .uUID(uuid)
            .requestTimestamp(0)
            .errorCodes(0)
            .splitMsg(SplitMsg.NULL_VAL);

        write();
    }

    public void readNewOrderSingle(final int expectedSeqNum)
    {
        final NewOrderSingle514Decoder newOrderSingle = read(new NewOrderSingle514Decoder(), 0);
        assertEquals(expectedSeqNum, newOrderSingle.seqNum());
        // TODO: newOrderSingle.sendingTimeEpoch()
    }

    public void assertDisconnected()
    {
        final boolean disconnected = testSystem.awaitBlocking(() ->
        {
            try
            {
                return socket.read(readBuffer) == -1;
            }
            catch (final IOException e)
            {
                return true;
            }
        });

        assertTrue(disconnected);
    }

    public void expectedUuid(final long lastUuid)
    {
        this.uuid = lastUuid;
    }

    public void readSequence(final long nextSeqNo, final KeepAliveLapsed keepAliveIntervalLapsed)
    {
        final Sequence506Decoder sequence = read(new Sequence506Decoder(), 0);

        assertEquals(uuid, sequence.uUID());
        assertEquals(nextSeqNo, sequence.nextSeqNo());
        assertEquals(FTI.Primary, sequence.faultToleranceIndicator());
        assertEquals(keepAliveIntervalLapsed, sequence.keepAliveIntervalLapsed());
    }

    public void writeSequence(final int nextSeqNo, final KeepAliveLapsed keepAliveLapsed)
    {
        final Sequence506Encoder sequence = new Sequence506Encoder();
        wrap(sequence, Sequence506Encoder.BLOCK_LENGTH);

        sequence
            .uUID(uuid)
            .nextSeqNo(nextSeqNo)
            .faultToleranceIndicator(FTI.Primary)
            .keepAliveIntervalLapsed(keepAliveLapsed);

        write();
    }

    public void writeNotApplied(final long fromSeqNo, final long msgCount)
    {
        final NotApplied513Encoder notApplied = new NotApplied513Encoder();
        wrap(notApplied, NotApplied513Encoder.BLOCK_LENGTH);

        notApplied
            .uUID(uuid)
            .fromSeqNo(fromSeqNo)
            .msgCount(msgCount)
            .splitMsg(SplitMsg.NULL_VAL);

        write();
    }

    public void writeExecutionReportStatus(final int sequenceNumber, final boolean possRetrans)
    {
        final ExecutionReportStatus532Encoder executionReportStatus = new ExecutionReportStatus532Encoder();
        wrap(executionReportStatus, ExecutionReportStatus532Encoder.BLOCK_LENGTH);

        executionReportStatus
            .seqNum(sequenceNumber)
            .uUID(uuid)
            .text("")
            .execID("123")
            .senderID(FIRM_ID)
            .clOrdID(CL_ORD_ID)
            .partyDetailsListReqID(1)
            .orderID(1)
            .transactTime(TimeUtil.microSecondTimestamp()) // TODO: nanos
            .sendingTimeEpoch(TimeUtil.microSecondTimestamp())
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

        executionReportStatus.price().mantissa(1);
        executionReportStatus.stopPx().mantissa(2);

        write();
    }

    public void acceptRetransRequest(final long fromSeqNo, final int msgCount)
    {
        final long requestTimestamp = readRetransmitRequest(fromSeqNo, msgCount);
        writeRetransmission(requestTimestamp, fromSeqNo, msgCount);
    }

    public void writeRetransmission(final long requestTimestamp, final long fromSeqNo, final int msgCount)
    {
        final Retransmission509Encoder retransmission = new Retransmission509Encoder();
        wrap(retransmission, Retransmission509Encoder.BLOCK_LENGTH);

        retransmission
            .uUID(uuid)
            .requestTimestamp(requestTimestamp)
            .fromSeqNo(fromSeqNo)
            .msgCount(msgCount);

        write();
    }

    public long readRetransmitRequest(final long fromSeqNo, final int msgCount)
    {
        final RetransmitRequest508Decoder retransmitRequest = read(new RetransmitRequest508Decoder(), 0);
        assertEquals(uuid, retransmitRequest.uUID());
        final long requestTimestamp = retransmitRequest.requestTimestamp();
        assertEquals(fromSeqNo, retransmitRequest.fromSeqNo());
        assertEquals(msgCount, retransmitRequest.msgCount());

        return requestTimestamp;
    }
}
