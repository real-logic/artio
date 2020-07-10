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

import iLinkBinary.ExecutionReportStatus532Decoder;
import org.agrona.DirectBuffer;
import org.agrona.collections.IntArrayList;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.library.NotAppliedResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static uk.co.real_logic.artio.LogTag.ILINK_SESSION;

public class FakeILink3ConnectionHandler implements ILink3ConnectionHandler
{
    private final IntArrayList messageIds = new IntArrayList();
    private final List<Exception> exceptions = new ArrayList<>();
    private final Consumer<NotAppliedResponse> notAppliedResponse;
    private final ExecutionReportStatus532Decoder executionReportStatus = new ExecutionReportStatus532Decoder();

    private boolean hasReceivedNotApplied;

    public FakeILink3ConnectionHandler(final Consumer<NotAppliedResponse> notAppliedResponse)
    {
        this.notAppliedResponse = notAppliedResponse;
    }

    public boolean hasReceivedNotApplied()
    {
        return hasReceivedNotApplied;
    }

    public void onBusinessMessage(
        final int templateId,
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final boolean possRetrans)
    {
        messageIds.add(templateId);
        if (templateId == ExecutionReportStatus532Decoder.TEMPLATE_ID)
        {
            executionReportStatus.wrap(buffer, offset, blockLength, version);
            DebugLogger.logSbeDecoder(ILINK_SESSION, "> ", executionReportStatus::appendTo);
        }
    }

    public void onNotApplied(
        final long fromSequenceNumber, final long msgCount, final NotAppliedResponse response)
    {
        hasReceivedNotApplied = true;
        notAppliedResponse.accept(response);
    }

    public void onRetransmitReject(
        final String reason, final long lastUuid, final long requestTimestamp, final int errorCodes)
    {

    }

    public void onSequence(final long uuid, final long nextSeqNo)
    {
    }

    public void onError(final Exception ex)
    {
        exceptions.add(ex);
    }

    public void onDisconnect()
    {

    }

    public IntArrayList messageIds()
    {
        return messageIds;
    }

    public List<Exception> exceptions()
    {
        return exceptions;
    }
}
