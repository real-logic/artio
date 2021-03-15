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
    private static final long KEEP_ALIVE_INTERVAL_IN_MS = 10_000L;
    public static final int CL_ORD_ID = 1;

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

    private long negotiateRequestTimestamp;

    public BinaryEntrypointClient(final int port, final TestSystem testSystem) throws IOException
    {
        socket = SocketChannel.open(new InetSocketAddress("localhost", port));
        this.testSystem = testSystem;

        headerDecoder.wrap(unsafeReadBuffer, SOFH_LENGTH);
    }

    public NegotiateResponseDecoder readNegotiateResponse()
    {
        return read(new NegotiateResponseDecoder(), 0);
    }

    public NegotiateRejectDecoder readNegotiateReject()
    {
        final NegotiateRejectDecoder reject = read(new NegotiateRejectDecoder(), 0);
        assertEquals(SESSION_ID, reject.sessionID());
        assertEquals(1, reject.sessionVerID());
        assertEquals(negotiateRequestTimestamp, reject.requestTimestamp().time());
        assertEquals(FIRM_ID, reject.enteringFirm());
        return reject;
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

            messageDecoder.wrap(
                unsafeReadBuffer,
                messageOffset,
                blockLength,
                version);

            print(unsafeReadBuffer, "> ");

            assertEquals(messageDecoder.sbeTemplateId(), templateId);

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

        negotiateRequestTimestamp = epochNanoClock.nanoTime();

        negotiate
            .sessionID(SESSION_ID)
            .sessionVerID(1)
            .timestamp().time(negotiateRequestTimestamp);
        negotiate
            .enteringFirm(FIRM_ID)
            .onbehalfFirm(NegotiateEncoder.onbehalfFirmNullValue())
            .senderLocation(SENDER_LOCATION);

        write();
    }

    public void writeEstablish()
    {
        final EstablishEncoder establish = new EstablishEncoder();
        wrap(establish, EstablishEncoder.BLOCK_LENGTH);

        establish
            .sessionID(SESSION_ID)
            .sessionVerID(1)
            .timestamp().time(epochNanoClock.nanoTime());
        establish.keepAliveInterval().time(KEEP_ALIVE_INTERVAL_IN_MS);
        establish
            .nextSeqNo(1)
            .cancelOnDisconnectType(CancelOnDisconnectType.DO_NOT_CANCEL_ON_DISCONNECT_OR_TERMINATE)
            .codTimeoutWindow().time(DeltaInMillisEncoder.timeNullValue());

        write();
    }

    public EstablishAckDecoder readEstablishAck()
    {
        return read(new EstablishAckDecoder(), 0);
    }

    public TerminateDecoder readTerminate()
    {
        final TerminateDecoder terminate = read(new TerminateDecoder(), 0);
        assertEquals(BinaryEntrypointClient.SESSION_ID, terminate.sessionID());
        assertEquals(1, terminate.sessionVerID());
        assertEquals(TerminationCode.FINISHED, terminate.terminationCode());
        return terminate;
    }

    public void readExecutionReportNew()
    {
        final ExecutionReport_NewDecoder report = read(new ExecutionReport_NewDecoder(), 0);
        assertEquals(CL_ORD_ID, report.clOrdID());
    }

    public void writeTerminate()
    {
        final TerminateEncoder terminate = new TerminateEncoder();
        wrap(terminate, TerminateEncoder.BLOCK_LENGTH);

        terminate
            .sessionID(SESSION_ID)
            .sessionVerID(1)
            .terminationCode(TerminationCode.FINISHED);

        write();
    }

    public void writeNewOrderSingle()
    {
        final NewOrderSingleEncoder newOrderSingle = new NewOrderSingleEncoder();
        wrap(newOrderSingle, NewOrderSingleEncoder.BLOCK_LENGTH);

        newOrderSingle
            .clOrdID(CL_ORD_ID)
            .securityID(2)
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
}
