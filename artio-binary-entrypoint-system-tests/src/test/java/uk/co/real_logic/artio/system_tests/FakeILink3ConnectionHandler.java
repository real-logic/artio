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

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.fixp.FixPConnection;
import uk.co.real_logic.artio.ilink.ILink3ConnectionHandler;
import uk.co.real_logic.artio.library.NotAppliedResponse;
import uk.co.real_logic.artio.messages.DisconnectReason;

public class FakeILink3ConnectionHandler implements ILink3ConnectionHandler
{
    public void onNotApplied(
        final FixPConnection connection,
        final long fromSequenceNumber, final long msgCount, final NotAppliedResponse response)
    {
    }

    public void onRetransmitReject(
        final FixPConnection connection, final String reason, final long requestTimestamp, final int errorCodes)
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
    }

    public void onDisconnect(final FixPConnection connection, final DisconnectReason reason)
    {
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
    }
}
