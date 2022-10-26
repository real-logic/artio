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

import iLinkBinary.ExecutionReportStatus532Decoder;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.collections.LongArrayList;
import uk.co.real_logic.artio.fixp.FixPConnection;
import uk.co.real_logic.artio.fixp.FixPMessageHeader;
import uk.co.real_logic.artio.ilink.ILink3ConnectionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

public class SequenceNumberCheckingHandler implements ILink3ConnectionHandler
{
    private final ExecutionReportStatus532Decoder executionReportStatus = new ExecutionReportStatus532Decoder();
    private final LongArrayList sequenceNumbers = new LongArrayList();
    private final LongArrayList uuids = new LongArrayList();
    private boolean retransmitTimedOut = false;

    public Action onBusinessMessage(
        final FixPConnection connection,
        final int templateId,
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final boolean possRetrans,
        final FixPMessageHeader messageHeader)
    {
        if (templateId == ExecutionReportStatus532Decoder.TEMPLATE_ID)
        {
            executionReportStatus.wrap(buffer, offset, blockLength, version);
            final long seqNum = executionReportStatus.seqNum();
            final long uuid = executionReportStatus.uUID();

            sequenceNumbers.add(seqNum);
            uuids.add(uuid);
        }

        return CONTINUE;
    }

    public LongArrayList sequenceNumbers()
    {
        return sequenceNumbers;
    }

    public LongArrayList uuids()
    {
        return uuids;
    }

    public Action onNotApplied(
        final FixPConnection connection,
        final long fromSequenceNumber,
        final long msgCount,
        final NotAppliedResponse response)
    {
        return CONTINUE;
    }

    public Action onRetransmitReject(
        final FixPConnection connection,
        final String reason,
        final long requestTimestamp,
        final int errorCodes)
    {
        return CONTINUE;
    }

    public boolean retransmitTimedOut()
    {
        return retransmitTimedOut;
    }

    public Action onRetransmitTimeout(final FixPConnection connection)
    {
        retransmitTimedOut = true;

        return CONTINUE;
    }

    public void resetRetransmitTimedOut()
    {
        retransmitTimedOut = false;
    }

    public Action onSequence(final FixPConnection connection, final long nextSeqNo)
    {
        return CONTINUE;
    }

    public Action onError(final FixPConnection connection, final Exception ex)
    {
        return CONTINUE;
    }

    public Action onDisconnect(final FixPConnection connection, final DisconnectReason reason)
    {
        return CONTINUE;
    }
}
