/*
 * Copyright 2015-2021 Real Logic Limited.
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
package uk.co.real_logic.artio.system_benchmarks;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.AsciiSequenceView;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.OrdType;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.builder.NewOrderSingleEncoder;
import uk.co.real_logic.artio.decoder.ExecutionReportDecoder;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.system_benchmarks.BenchmarkConfiguration.MESSAGES_EXCHANGED;

public final class ClientBenchmarkSessionHandler implements SessionHandler
{
    private static final int SIZE_OF_ASCII_LONG = String.valueOf(Long.MAX_VALUE).length();

    private static final byte[] CL_ORD_ID_BUFFER = new byte[SIZE_OF_ASCII_LONG];
    private static final UnsafeBuffer CL_ORD_ID_ENCODER = new UnsafeBuffer(CL_ORD_ID_BUFFER);

    private static int lastClOrdId = -1;

    private static final long[] SEND_TIMES_IN_NS = new long[MESSAGES_EXCHANGED];
    private static final long[] LATENCIES_IN_NS = new long[MESSAGES_EXCHANGED];

    private final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final ExecutionReportDecoder executionReport = new ExecutionReportDecoder();
    private final NewOrderSingleEncoder newOrderSingle = new NewOrderSingleEncoder();
    private final AsciiSequenceView orderIdView = new AsciiSequenceView();

    private final DecimalFloat price = new DecimalFloat(100);
    private final DecimalFloat orderQty = new DecimalFloat(2);
    private final UtcTimestampEncoder transactTime = new UtcTimestampEncoder();
    private final Session session;

    public ClientBenchmarkSessionHandler(final Session session, final EpochNanoClock epochNanoClock)
    {
        this.session = session;
//        this.epochNanoClock = epochNanoClock;

        setupEncoder();
    }

    private void setupEncoder()
    {
        newOrderSingle
            .side(Side.BUY)
            .ordType(OrdType.MARKET)
            .price(price);

        newOrderSingle.instrument().symbol("MSFT");
        newOrderSingle.orderQtyData().orderQty(orderQty);
    }

    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final Session session,
        final int sequenceIndex,
        final long messageType,
        final long timestamp,
        final long position,
        final OnMessageInfo messageInfo)
    {
        if (messageType != ExecutionReportDecoder.MESSAGE_TYPE)
        {
            return CONTINUE;
        }

        final long timestampInNs = System.nanoTime();

        final MutableAsciiBuffer asciiBuffer = this.asciiBuffer;
        final ExecutionReportDecoder executionReport = this.executionReport;

        asciiBuffer.wrap(buffer);
        executionReport.decode(asciiBuffer, offset, length);
        executionReport.orderID(orderIdView);

        final int orderId = orderIdView.buffer().parseIntAscii(orderIdView.offset(), orderIdView.length());
        final long startTimeInNs = SEND_TIMES_IN_NS[orderId];
        if (startTimeInNs == 0)
        {
            System.err.println("Invalid start time for: " + orderId);
        }

        final long roundTripInNs = timestampInNs - startTimeInNs;
        if (roundTripInNs < 0)
        {
            System.err.println("Invalid round trip timing: " + roundTripInNs + " for " + orderId);
        }

        LATENCIES_IN_NS[orderId] = roundTripInNs;

        return CONTINUE;
    }

    public boolean trySend()
    {
        final int transactTimeLength = transactTime.encode(System.currentTimeMillis());

        final int clOrdId = ClientBenchmarkSessionHandler.lastClOrdId + 1;
        final int clOrdIdEncodedLength = CL_ORD_ID_ENCODER.putLongAscii(0, clOrdId);

        newOrderSingle
            .clOrdID(CL_ORD_ID_BUFFER, clOrdIdEncodedLength)
            .transactTime(transactTime.buffer(), transactTimeLength);

        final long nanoTime = System.nanoTime(); /*epochNanoClock.nanoTime();*/
        if (Pressure.isBackPressured(session.trySend(newOrderSingle)))
        {
            return false;
        }

        SEND_TIMES_IN_NS[clOrdId] = nanoTime;
        lastClOrdId = clOrdId;
        return true;
    }

    public void onTimeout(final int libraryId, final Session session)
    {
        System.out.println("BenchmarkSessionHandler.onTimeout: " + session.id());
    }

    public void onSlowStatus(final int libraryId, final Session session, final boolean hasBecomeSlow)
    {
        System.out.println(
            "sessionId = " + session.id() +
            (hasBecomeSlow ? " became slow" : " became not slow") +
            ", lastReceivedMsgSeqNum = " + session.lastReceivedMsgSeqNum() +
            ", lastSentMsgSeqNum = " + session.lastSentMsgSeqNum());
    }

    public Action onDisconnect(final int libraryId, final Session session, final DisconnectReason reason)
    {
        System.out.printf("%d disconnected due to %s%n", session.id(), reason);

        return CONTINUE;
    }

    public void onSessionStart(final Session session)
    {
    }

    public static boolean isComplete()
    {
        return LATENCIES_IN_NS[MESSAGES_EXCHANGED - 1] != 0;
    }

    public static void dumpLatencies()
    {
        try (PrintStream csv = new PrintStream(new FileOutputStream("latencies.csv")))
        {
            csv.println("Time, Latency");

            for (int i = 0; i < MESSAGES_EXCHANGED; i++)
            {
                final long sendTimeInNs = SEND_TIMES_IN_NS[i];
                csv.println(TimeUnit.NANOSECONDS.toSeconds(sendTimeInNs) + ", " + LATENCIES_IN_NS[i]);
            }
            csv.flush();
        }
        catch (final IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    public boolean isActive()
    {
        return session.isActive();
    }
}
