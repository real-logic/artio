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

import org.agrona.DirectBuffer;
import org.agrona.collections.IntArrayList;
import uk.co.real_logic.artio.library.NotAppliedResponse;

import java.util.function.Consumer;

public class FakeILink3SessionHandler implements ILink3SessionHandler
{
    private final Consumer<NotAppliedResponse> notAppliedResponse;
    private boolean hasReceivedNotApplied;
    private IntArrayList messageIds = new IntArrayList();

    public FakeILink3SessionHandler(final Consumer<NotAppliedResponse> notAppliedResponse)
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
    }

    public void onNotApplied(
        final long fromSequenceNumber, final long msgCount, final NotAppliedResponse response)
    {
        hasReceivedNotApplied = true;
        notAppliedResponse.accept(response);
    }

    public void onRetransmitReject(final String reason, final long requestTimestamp, final int errorCodes)
    {

    }

    public IntArrayList messageIds()
    {
        return messageIds;
    }
}
