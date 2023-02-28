/*
 * Copyright 2015-2023 Real Logic Limited.
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
package uk.co.real_logic.artio.admin;

import io.aeron.ExclusivePublication;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.messages.AdminResetSequenceNumbersRequestEncoder;
import uk.co.real_logic.artio.messages.AllFixSessionsRequestEncoder;
import uk.co.real_logic.artio.messages.DisconnectSessionRequestEncoder;
import uk.co.real_logic.artio.protocol.ClaimablePublication;

/**
 * A proxy for publishing messages fix related messages
 */
class AdminPublication extends ClaimablePublication
{
    private static final int ALL_FIX_SESSIONS_REQUEST_LENGTH =
        HEADER_LENGTH + AllFixSessionsRequestEncoder.BLOCK_LENGTH;
    private static final int DISCONNECT_SESSION_REQUEST_LENGTH =
        HEADER_LENGTH + DisconnectSessionRequestEncoder.BLOCK_LENGTH;
    private static final int RESET_SEQUENCE_NUMBERS_REQUEST_LENGTH =
        HEADER_LENGTH + AdminResetSequenceNumbersRequestEncoder.BLOCK_LENGTH;

    private final AllFixSessionsRequestEncoder allFixSessionsRequest = new AllFixSessionsRequestEncoder();
    private final DisconnectSessionRequestEncoder disconnectSessionRequest = new DisconnectSessionRequestEncoder();
    private final AdminResetSequenceNumbersRequestEncoder adminResetSequenceNumbersRequest =
        new AdminResetSequenceNumbersRequestEncoder();

    AdminPublication(
        final ExclusivePublication dataPublication,
        final AtomicCounter fails,
        final IdleStrategy idleStrategy,
        final int maxClaimAttempts)
    {
        super(maxClaimAttempts, idleStrategy, fails, dataPublication);
    }

    long saveRequestAllFixSessions(final long correlationId)
    {
        final long position = claim(ALL_FIX_SESSIONS_REQUEST_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        allFixSessionsRequest
            .wrapAndApplyHeader(buffer, offset, header)
            .correlationId(correlationId);

        bufferClaim.commit();

        return position;
    }

    long saveDisconnectSession(final long correlationId, final long sessionId)
    {
        final long position = claim(DISCONNECT_SESSION_REQUEST_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        disconnectSessionRequest
            .wrapAndApplyHeader(buffer, offset, header)
            .correlationId(correlationId)
            .sessionId(sessionId);

        bufferClaim.commit();

        return position;
    }

    long saveResetSequenceNumbers(final long correlationId, final long sessionId)
    {
        final long position = claim(RESET_SEQUENCE_NUMBERS_REQUEST_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        adminResetSequenceNumbersRequest
            .wrapAndApplyHeader(buffer, offset, header)
            .correlationId(correlationId)
            .sessionId(sessionId);

        bufferClaim.commit();

        return position;
    }
}
