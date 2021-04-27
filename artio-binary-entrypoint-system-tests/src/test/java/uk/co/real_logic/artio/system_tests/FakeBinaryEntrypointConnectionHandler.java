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

import b3.entrypoint.fixp.sbe.*;
import org.agrona.DirectBuffer;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import uk.co.real_logic.artio.fixp.FixPConnection;
import uk.co.real_logic.artio.fixp.FixPConnectionHandler;
import uk.co.real_logic.artio.library.NotAppliedResponse;
import uk.co.real_logic.artio.messages.DisconnectReason;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class FakeBinaryEntrypointConnectionHandler implements FixPConnectionHandler
{
    private final IntArrayList messageIds = new IntArrayList();
    private final LongArrayList sessionIds = new LongArrayList();
    private final List<Exception> exceptions = new ArrayList<>();
    private final Consumer<NotAppliedResponse> notAppliedResponse;

    private DisconnectReason disconnectReason;
    private boolean replyToOrder = true;
    private boolean abortReport;
    private boolean finishedSending = false;
    private long lastPosition;

    public FakeBinaryEntrypointConnectionHandler()
    {
        this.notAppliedResponse = ignore -> {};
    }

    public void abortReport(final boolean abortReport)
    {
        this.abortReport = abortReport;
    }

    public void replyToOrder(final boolean replyToOrder)
    {
        this.replyToOrder = replyToOrder;
    }

    public boolean hasFinishedSending()
    {
        return finishedSending;
    }

    public void onBusinessMessage(
        final FixPConnection connection,
        final int templateId,
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final boolean possRetrans)
    {
        messageIds.add(templateId);
        sessionIds.add(connection.key().sessionIdIfExists());

        if (replyToOrder && templateId == NewOrderSingleDecoder.TEMPLATE_ID)
        {
            final NewOrderSingleDecoder newOrderSingle = new NewOrderSingleDecoder();
            newOrderSingle.wrap(buffer, offset, blockLength, version);

            final long clOrderID = newOrderSingle.clOrdID();
            final long securityID = newOrderSingle.securityID();

            lastPosition = sendExecutionReportNew(connection, clOrderID, securityID, abortReport);
        }
    }

    public void onFinishedSending(final FixPConnection connection)
    {
        finishedSending = true;
    }

    static long sendExecutionReportNew(
        final FixPConnection connection, final long clOrderID, final long securityID, final boolean abortReport)
    {
        final ExecutionReport_NewEncoder executionReport = new ExecutionReport_NewEncoder();

        long position;

        while (true)
        {
            position = connection.tryClaim(executionReport);
            if (position >= 0)
            {
                break;
            }
            else
            {
                Thread.yield();
            }
        }

        executionReport
            .orderID(clOrderID)
            .clOrdID(clOrderID)
            .securityID(securityID)
            .secondaryOrderID(ExecutionReport_NewEncoder.secondaryOrderIDNullValue())
            .ordStatus(OrdStatus.NEW)
            .execRestatementReason(ExecRestatementReason.NULL_VAL)
            .multiLegReportingType(MultiLegReportingType.NULL_VAL)
            .workingIndicator(Bool.NULL_VAL)
            .transactTime().time(System.nanoTime());
        executionReport
            .putTradeDate(1, 2)
            .protectionPrice().mantissa(1234);
        executionReport.receivedTime().time(System.nanoTime());

        if (abortReport)
        {
            connection.abort();
        }
        else
        {
            connection.commit();
        }

        return position;
    }

    public void onNotApplied(
        final FixPConnection connection,
        final long fromSequenceNumber,
        final long msgCount,
        final NotAppliedResponse response)
    {
        notAppliedResponse.accept(response);
    }

    public void onRetransmitReject(
        final FixPConnection connection,
        final String reason,
        final long requestTimestamp,
        final int errorCodes)
    {
    }

    public void onRetransmitTimeout(final FixPConnection connection)
    {
    }

    public void onSequence(final FixPConnection connection, final long nextSeqNo)
    {
    }

    public void onError(final FixPConnection connection, final Exception ex)
    {
        exceptions.add(ex);
    }

    public void onDisconnect(final FixPConnection connection, final DisconnectReason reason)
    {
        this.disconnectReason = reason;
    }

    public DisconnectReason disconnectReason()
    {
        return disconnectReason;
    }

    public IntArrayList templateIds()
    {
        return messageIds;
    }

    public LongArrayList sessionIds()
    {
        return sessionIds;
    }

    public List<Exception> exceptions()
    {
        return exceptions;
    }

    public long lastPosition()
    {
        return lastPosition;
    }

    public void reset()
    {
        lastPosition = 0;
        disconnectReason = null;
        messageIds.clear();
        exceptions.clear();
        sessionIds.clear();
    }
}
