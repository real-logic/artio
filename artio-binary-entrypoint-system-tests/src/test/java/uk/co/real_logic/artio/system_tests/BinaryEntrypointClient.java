/*
 * Copyright 2021 Monotonic Ltd.
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
package uk.co.real_logic.artio.system_tests;

import b3.entrypoint.fixp.sbe.*;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.SystemEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageDecoderFlyweight;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointOffsets;
import uk.co.real_logic.sbe.json.JsonPrinter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.LogTag.FIX_TEST;
import static uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointProxy.BINARY_ENTRYPOINT_HEADER_LENGTH;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.*;

public final class BinaryEntrypointClient implements AutoCloseable
{
    private static final int NOT_SKIPPING = -1;
    private static final int OFFSET = 0;

    public static final int BUFFER_SIZE = 8 * 1024;
    public static final int SESSION_ID = 123;
    public static final int FIRM_ID = 456;
    public static final String SENDER_LOCATION = "LOCATION_1";
    public static final int CL_ORD_ID = 1;
    private static final long KEEP_ALIVE_INTERVAL_IN_MS = 10_000L;
    public static final long SECURITY_ID = 2;

    private final JsonPrinter jsonPrinter = new JsonPrinter(BinaryEntryPointOffsets.loadSbeIr());

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final UnsafeBuffer unsafeWriteBuffer = new UnsafeBuffer(writeBuffer);

    private final ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final UnsafeBuffer unsafeReadBuffer = new UnsafeBuffer(readBuffer);

    private final EpochNanoClock epochNanoClock = new SystemEpochNanoClock();

    private final SocketChannel socket;
    private final TestSystem testSystem;

    private int skipTemplateId = NOT_SKIPPING;
    private long sessionVerID = 1;
    private long negotiateTimestampInNs;
    private long establishTimestampInNs;
    private long retransmitRequestTimestampInNs;

    private long keepAliveIntervalInMs = KEEP_ALIVE_INTERVAL_IN_MS;

    public BinaryEntrypointClient(final int port, final TestSystem testSystem) throws IOException
    {
        socket = SocketChannel.open(new InetSocketAddress("localhost", port));
        this.testSystem = testSystem;

        headerDecoder.wrap(unsafeReadBuffer, SOFH_LENGTH);
    }

    public BinaryEntrypointClient sessionVerID(final long sessionVerID)
    {
        this.sessionVerID = sessionVerID;
        return this;
    }

    public void skipTemplateId(final int skipTemplateId)
    {
        this.skipTemplateId = skipTemplateId;
    }

    public void skipSequence()
    {
        skipTemplateId(SequenceDecoder.TEMPLATE_ID);
    }

    public void dontSkip()
    {
        skipTemplateId(NOT_SKIPPING);
    }

    public long sessionVerID()
    {
        return sessionVerID;
    }

    public void keepAliveIntervalInMs(final long keepAliveIntervalInMs)
    {
        this.keepAliveIntervalInMs = keepAliveIntervalInMs;
    }

    public NegotiateResponseDecoder readNegotiateResponse()
    {
        final NegotiateResponseDecoder response = read(new NegotiateResponseDecoder(), 0);
        assertEquals(BinaryEntrypointClient.SESSION_ID, response.sessionID());
        assertEquals(sessionVerID, response.sessionVerID());
        assertEquals(BinaryEntrypointClient.FIRM_ID, response.enteringFirm());
        return response;
    }

    public NegotiateRejectDecoder readNegotiateReject(final NegotiationRejectCode negotiationRejectCode)
    {
        final NegotiateRejectDecoder reject = read(new NegotiateRejectDecoder(), 0);
        assertEquals(SESSION_ID, reject.sessionID());
        assertEquals(sessionVerID, reject.sessionVerID());
        assertEquals(negotiateTimestampInNs, reject.requestTimestamp().time());
        assertEquals(FIRM_ID, reject.enteringFirm());
        assertEquals(negotiationRejectCode, reject.negotiationRejectCode());
        return reject;
    }

    public void readEstablishReject(final EstablishRejectCode rejectCode)
    {
        final EstablishRejectDecoder reject = read(new EstablishRejectDecoder(), 0);
        assertEquals(SESSION_ID, reject.sessionID());
        assertEquals(sessionVerID, reject.sessionVerID());
        assertEquals(establishTimestampInNs, reject.requestTimestamp().time());
        assertEquals(rejectCode, reject.establishmentRejectCode());
    }

    public <T extends MessageDecoderFlyweight> T read(final T messageDecoder, final int nonBlockLength)
    {
        return testSystem.awaitBlocking(() -> readInternal(messageDecoder, nonBlockLength));
    }

    private <T extends MessageDecoderFlyweight> T readInternal(final T messageDecoder, final int nonBlockLength)
    {
        try
        {
            final int headerLength = MessageHeaderDecoder.ENCODED_LENGTH + SOFH_LENGTH;
            readBuffer.limit(headerLength);
            final int readHeader = socket.read(readBuffer);
            if (readHeader != headerLength)
            {
                throw new IllegalStateException("readHeader=" + readHeader + ",headerLength" + headerLength);
            }

            final int totalLength = readSofh(unsafeReadBuffer, 0, BINARY_ENTRYPOINT_TYPE);
            final int templateId = headerDecoder.templateId();
            final int blockLength = headerDecoder.blockLength();
            final int version = headerDecoder.version();

            if (skipTemplateId != NOT_SKIPPING && templateId == skipTemplateId)
            {
                readBuffer.limit(totalLength);
                int readSkip = 0;
                do
                {
                    readSkip += socket.read(readBuffer);
                }
                while (readSkip < (totalLength - readHeader));

                readBuffer.clear();

                return readInternal(messageDecoder, nonBlockLength);
            }

            final int messageOffset = headerLength;
            final int expectedLength = messageOffset + messageDecoder.sbeBlockLength() + nonBlockLength;
            readBuffer.limit(expectedLength);

            final int readBody = socket.read(readBuffer);
            final int read = readHeader + readBody;

            print(unsafeReadBuffer, "> ");

            final int decodedTemplateId = messageDecoder.sbeTemplateId();
            if (decodedTemplateId != templateId)
            {
                String msg = "invalid template id: ";

                if (templateId == EstablishRejectDecoder.TEMPLATE_ID)
                {
                    final EstablishRejectDecoder establishRejectDecoder = new EstablishRejectDecoder();
                    establishRejectDecoder.wrap(
                        unsafeReadBuffer,
                        messageOffset,
                        blockLength,
                        version);
                    msg += establishRejectDecoder.toString();
                }
                else if (templateId == EstablishAckDecoder.TEMPLATE_ID)
                {
                    msg += "received Establish Ack";
                }
                else if (templateId == ExecutionReport_NewDecoder.TEMPLATE_ID)
                {
                    msg += "received Execution Report New";
                }

                assertEquals(msg, decodedTemplateId, templateId);
            }

            if (totalLength != read)
            {
                throw new IllegalArgumentException("totalLength=" + totalLength + ",read=" + read);
            }

            messageDecoder.wrap(
                unsafeReadBuffer,
                messageOffset,
                blockLength,
                version);

            assertThat(read, greaterThanOrEqualTo(messageOffset + blockLength));

            readBuffer.clear();

            return messageDecoder;
        }
        catch (final IOException e)
        {
            LangUtil.rethrowUnchecked(e);
            return null;
        }
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

    private void wrap(final MessageEncoderFlyweight messageEncoder, final int length)
    {
        final int messageSize = BINARY_ENTRYPOINT_HEADER_LENGTH + length;
        writeBinaryEntryPointSofh(unsafeWriteBuffer, 0, messageSize);
        DebugLogger.log(FIX_TEST, "wrap messageSize=", String.valueOf(messageSize));

        headerEncoder
            .wrap(unsafeWriteBuffer, SOFH_LENGTH)
            .blockLength(messageEncoder.sbeBlockLength())
            .templateId(messageEncoder.sbeTemplateId())
            .schemaId(messageEncoder.sbeSchemaId())
            .version(messageEncoder.sbeSchemaVersion());

        messageEncoder.wrap(unsafeWriteBuffer, BINARY_ENTRYPOINT_HEADER_LENGTH);
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

    public void close()
    {
        CloseHelper.close(socket);
    }

    public void writeNegotiate()
    {
        final NegotiateEncoder negotiate = new NegotiateEncoder();
        wrap(negotiate, NegotiateEncoder.BLOCK_LENGTH);

        negotiateTimestampInNs = timeInNs();

        negotiate
            .sessionID(SESSION_ID)
            .sessionVerID(sessionVerID)
            .timestamp().time(negotiateTimestampInNs);
        negotiate
            .enteringFirm(FIRM_ID)
            .onbehalfFirm(NegotiateEncoder.onbehalfFirmNullValue())
            .senderLocation(SENDER_LOCATION);

        write();
    }

    public void writeEstablish()
    {
        writeEstablish(1);
    }

    public void writeEstablish(final int nextSeqNo)
    {
        final EstablishEncoder establish = new EstablishEncoder();
        wrap(establish, EstablishEncoder.BLOCK_LENGTH);

        establishTimestampInNs = timeInNs();
        establish
            .sessionID(SESSION_ID)
            .sessionVerID(sessionVerID)
            .timestamp().time(establishTimestampInNs);
        establish.keepAliveInterval().time(keepAliveIntervalInMs);
        establish
            .nextSeqNo(nextSeqNo)
            .cancelOnDisconnectType(CancelOnDisconnectType.DO_NOT_CANCEL_ON_DISCONNECT_OR_TERMINATE)
            .codTimeoutWindow().time(DeltaInMillisEncoder.timeNullValue());

        write();
    }

    public EstablishAckDecoder readFirstEstablishAck()
    {
        return readEstablishAck(1, 0);
    }

    public EstablishAckDecoder readEstablishAck(final int nextSeqNo, final int lastIncomingSeqNo)
    {
        final EstablishAckDecoder establishAck = read(new EstablishAckDecoder(), 0);
        assertEquals("sessionID", BinaryEntrypointClient.SESSION_ID, establishAck.sessionID());
        assertEquals("sessionVerID", sessionVerID, establishAck.sessionVerID());
        assertEquals("nextSeqNo", nextSeqNo, establishAck.nextSeqNo());
        assertEquals("lastIncomingSeqNo", lastIncomingSeqNo, establishAck.lastIncomingSeqNo());
        return establishAck;
    }

    public TerminateDecoder readTerminate()
    {
        return readTerminate(TerminationCode.FINISHED);
    }

    public TerminateDecoder readTerminate(final TerminationCode terminationCode)
    {
        final TerminateDecoder terminate = read(new TerminateDecoder(), 0);
        assertEquals(BinaryEntrypointClient.SESSION_ID, terminate.sessionID());
        assertEquals(sessionVerID, terminate.sessionVerID());
        assertEquals(terminationCode, terminate.terminationCode());
        return terminate;
    }

    public void readExecutionReportNew()
    {
        readExecutionReportNew(CL_ORD_ID);
    }

    public void readExecutionReportNew(final int clOrdId)
    {
        final ExecutionReport_NewDecoder report = read(new ExecutionReport_NewDecoder(), 0);
        assertEquals(clOrdId, report.clOrdID());
    }

    public void writeTerminate()
    {
        final TerminateEncoder terminate = new TerminateEncoder();
        wrap(terminate, TerminateEncoder.BLOCK_LENGTH);

        terminate
            .sessionID(SESSION_ID)
            .sessionVerID(sessionVerID)
            .terminationCode(TerminationCode.FINISHED);

        write();
    }

    public void writeNewOrderSingle()
    {
        writeNewOrderSingle(CL_ORD_ID);
    }

    public void writeNewOrderSingle(final int clOrdId)
    {
        final NewOrderSingleEncoder newOrderSingle = new NewOrderSingleEncoder();
        wrap(newOrderSingle, NewOrderSingleEncoder.BLOCK_LENGTH);

        newOrderSingle
            .clOrdID(clOrdId)
            .securityID(SECURITY_ID)
            .price().mantissa(3);
        newOrderSingle
            .putOrderQty(1, 2, 3, 4)
            .account(5)
            .marketSegmentID(NewOrderSingleEncoder.marketSegmentIDNullValue())
            .side(Side.BUY)
            .ordType(OrdType.MARKET)
            .timeInForce(TimeInForce.FILL_OR_KILL)
            .stopPx().mantissa(PriceOptionalEncoder.mantissaNullValue());
        newOrderSingle
            .putMinQty(1, 2, 3, 4)
            .putMaxFloor(5, 6, 7, 8)
            .enteringTrader("Maria")
            .ordTagID((short)1)
            .mmProtectionReset(Bool.TRUE_VALUE)
            .routingInstruction(RoutingInstruction.NULL_VAL)
            .putExpireDate(5, 5)
            .investorID(123)
            .custodianInfo()
                .custodian(1)
                .custodyAccount(2)
                .custodyAllocationType(3);

        write();
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

        try
        {
            socket.close();
        }
        catch (final IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    public void writeFinishedSending(final long lastSeqNo)
    {
        final FinishedSendingEncoder finishedSending = new FinishedSendingEncoder();
        wrap(finishedSending, FinishedSendingEncoder.BLOCK_LENGTH);

        finishedSending
            .sessionID(SESSION_ID)
            .sessionVerID(sessionVerID)
            .lastSeqNo(lastSeqNo);

        write();
    }

    public void readFinishedReceiving()
    {
        final FinishedReceivingDecoder finishedReceiving = read(new FinishedReceivingDecoder(), 0);
        assertEquals(SESSION_ID, finishedReceiving.sessionID());
        assertEquals(sessionVerID, finishedReceiving.sessionVerID());
    }

    public void readFinishedSending(final int lastSeqNo)
    {
        final FinishedSendingDecoder finishedSending = read(new FinishedSendingDecoder(), 0);
        assertEquals(SESSION_ID, finishedSending.sessionID());
        assertEquals(sessionVerID, finishedSending.sessionVerID());
        assertEquals(lastSeqNo, finishedSending.lastSeqNo());
    }

    public void readSequence(final long nextSeqNo)
    {
        final SequenceDecoder sequence = read(new SequenceDecoder(), 0);
        assertEquals(nextSeqNo, sequence.nextSeqNo());
    }

    public void writeFinishedReceiving()
    {
        final FinishedReceivingEncoder finishedReceiving = new FinishedReceivingEncoder();
        wrap(finishedReceiving, FinishedReceivingEncoder.BLOCK_LENGTH);

        finishedReceiving
            .sessionID(SESSION_ID)
            .sessionVerID(sessionVerID);

        write();
    }

    public void writeRetransmitRequest(final long fromSeqNo, final long count)
    {
        writeRetransmitRequest(SESSION_ID, fromSeqNo, count);
    }

    public void writeRetransmitRequest(final int sessionId, final long fromSeqNo, final long count)
    {
        writeRetransmitRequest(sessionId, fromSeqNo, count, timeInNs());
    }

    public void writeRetransmitRequest(final int sessionId, final long fromSeqNo, final long count, final long timeInNs)
    {
        final RetransmitRequestEncoder retransmitRequest = new RetransmitRequestEncoder();
        wrap(retransmitRequest, RetransmitRequestEncoder.BLOCK_LENGTH);

        retransmitRequestTimestampInNs = timeInNs;

        retransmitRequest
            .sessionID(sessionId)
            .timestamp().time(retransmitRequestTimestampInNs);
        retransmitRequest
            .fromSeqNo(fromSeqNo)
            .count(count);

        write();
    }

    public void writeSequence(final int nextSeqNo)
    {
        final SequenceEncoder sequence = new SequenceEncoder();
        wrap(sequence, SequenceEncoder.BLOCK_LENGTH);

        sequence.nextSeqNo(nextSeqNo);

        write();
    }

    public void readNotApplied(final int fromSeqNo, final int count)
    {
        final NotAppliedDecoder notApplied = read(new NotAppliedDecoder(), 0);
        assertEquals(fromSeqNo, notApplied.fromSeqNo());
        assertEquals(count, notApplied.count());
    }

    public void readRetransmitReject(final RetransmitRejectCode rejectCode)
    {
        final RetransmitRejectDecoder retransmitReject = read(new RetransmitRejectDecoder(), 0);
        assertEquals(SESSION_ID, retransmitReject.sessionID());
        assertEquals(retransmitRequestTimestampInNs, retransmitReject.requestTimestamp().time());
        assertEquals(rejectCode, retransmitReject.retransmitRejectCode());
    }

    public void readRetransmission(final long nextSeqNo, final long count)
    {
        final RetransmissionDecoder retransmission = read(new RetransmissionDecoder(), 0);
        assertEquals(SESSION_ID, retransmission.sessionID());
        assertEquals(nextSeqNo, retransmission.nextSeqNo());
        assertEquals(count, retransmission.count());
        assertEquals(retransmitRequestTimestampInNs, retransmission.requestTimestamp().time());
    }

    public long timeInNs()
    {
        return epochNanoClock.nanoTime();
    }
}
