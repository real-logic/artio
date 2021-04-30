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
package uk.co.real_logic.artio.system_tests;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.ExecType;
import uk.co.real_logic.artio.OrdStatus;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.builder.ExecutionReportEncoder;
import uk.co.real_logic.artio.session.Session;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class ReportFactory
{
    private static final int SIZE_OF_ASCII_LONG = String.valueOf(Long.MAX_VALUE).length();
    public static final String MSFT = "MSFT";

    private final ExecutionReportEncoder executionReport = new ExecutionReportEncoder();
    private final byte[] encodeBuffer = new byte[SIZE_OF_ASCII_LONG];
    private final UnsafeBuffer encoder = new UnsafeBuffer(encodeBuffer);

    public Action trySendReportAct(final Session session, final Side side)
    {
        setupReport(side, session.lastSentMsgSeqNum());

        return Pressure.apply(session.trySend(executionReport));
    }

    public long trySendReport(final Session session, final Side side)
    {
        setupReport(side, session.lastSentMsgSeqNum());

        return session.trySend(executionReport);
    }

    public void sendReport(final TestSystem testSystem, final Session session, final Side side)
    {
        testSystem.awaitSend(() -> trySendReport(session, Side.SELL));
    }

    public static void sendOneReport(final TestSystem testSystem, final Session session, final Side side)
    {
        new ReportFactory().sendReport(testSystem, session, side);
    }

    public ExecutionReportEncoder setupReport(final Side side, final int execAndOrderId)
    {
        final int encodedLength = encoder.putLongAscii(0, execAndOrderId);

        executionReport.orderID(encodeBuffer, encodedLength)
            .execID(encodeBuffer, encodedLength);

        executionReport
            .execType(ExecType.FILL)
            .ordStatus(OrdStatus.FILLED)
            .side(side);

        executionReport.instrument().symbol(MSFT.getBytes(US_ASCII));

        return executionReport;
    }
}
