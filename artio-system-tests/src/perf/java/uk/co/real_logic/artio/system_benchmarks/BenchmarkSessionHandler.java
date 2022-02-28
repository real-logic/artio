/*
 * Copyright 2015-2022 Real Logic Limited.
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
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.ExecType;
import uk.co.real_logic.artio.OrdStatus;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.builder.ExecutionReportEncoder;
import uk.co.real_logic.artio.decoder.NewOrderSingleDecoder;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

public final class BenchmarkSessionHandler implements SessionHandler
{
    private static final int SIZE_OF_ASCII_LONG = String.valueOf(Long.MAX_VALUE).length();

    private static final byte[] EXEC_ID_BUFFER = new byte[SIZE_OF_ASCII_LONG];
    private static final UnsafeBuffer EXEC_ID_ENCODER = new UnsafeBuffer(EXEC_ID_BUFFER);
    private static long execId = 0;

    private final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final ExecutionReportEncoder executionReport = new ExecutionReportEncoder();
    private final NewOrderSingleDecoder newOrderSingle = new NewOrderSingleDecoder();

    public BenchmarkSessionHandler()
    {
        setupEncoder();
    }

    private void setupEncoder()
    {
        executionReport
            .execType(ExecType.FILL)
            .ordStatus(OrdStatus.FILLED)
            .side(Side.BUY);

        executionReport.instrument().symbol("MSFT");
    }

    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final Session session,
        final int sequenceIndex,
        final long messageType,
        final long timestampInNs,
        final long position,
        final OnMessageInfo messageInfo)
    {
        if (messageType != NewOrderSingleDecoder.MESSAGE_TYPE)
        {
            return CONTINUE;
        }

        return replyToOrder(buffer, offset, length, session);
    }

    private Action replyToOrder(
        final DirectBuffer buffer, final int offset, final int length, final Session session)
    {
        final MutableAsciiBuffer asciiBuffer = this.asciiBuffer;
        final NewOrderSingleDecoder newOrderSingle = this.newOrderSingle;
        final ExecutionReportEncoder executionReport = this.executionReport;

        asciiBuffer.wrap(buffer);
        newOrderSingle.decode(asciiBuffer, offset, length);

        final long execId = BenchmarkSessionHandler.execId + 1;
        final int execIdEncodedLength = EXEC_ID_ENCODER.putLongAscii(0, execId);

        executionReport
            .orderID(newOrderSingle.clOrdID(), newOrderSingle.clOrdIDLength())
            .execID(EXEC_ID_BUFFER, execIdEncodedLength);

        if (Pressure.isBackPressured(session.trySend(executionReport)))
        {
            return ABORT;
        }

        BenchmarkSessionHandler.execId = execId;
        return CONTINUE;
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

}
