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
package uk.co.real_logic.artio.engine.framer;

import io.aeron.ExclusivePublication;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.messages.AllFixSessionsReplyEncoder;
import uk.co.real_logic.artio.messages.GenericAdminReplyEncoder;
import uk.co.real_logic.artio.messages.GatewayError;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;
import uk.co.real_logic.artio.protocol.ClaimablePublication;

/**
 * A proxy for publishing messages fix related messages
 */
class AdminReplyPublication extends ClaimablePublication
{
    private static final int GENERIC_ADMIN_REPLY_LENGTH = HEADER_LENGTH + GenericAdminReplyEncoder.BLOCK_LENGTH +
        GenericAdminReplyEncoder.messageHeaderLength();

    private final ExpandableArrayBuffer expandableArrayBuffer = new ExpandableArrayBuffer();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final AllFixSessionsReplyEncoder allFixSessionsReply = new AllFixSessionsReplyEncoder();
    private final GenericAdminReplyEncoder genericAdminReply = new GenericAdminReplyEncoder();

    AdminReplyPublication(
        final ExclusivePublication dataPublication,
        final AtomicCounter fails,
        final IdleStrategy idleStrategy,
        final int maxClaimAttempts)
    {
        super(maxClaimAttempts, idleStrategy, fails, dataPublication);
    }

    AllFixSessionsReplyEncoder.SessionsEncoder startRequestAllFixSessions(
        final long correlationId,
        final int sessionsCount)
    {
        allFixSessionsReply.wrapAndApplyHeader(expandableArrayBuffer, 0, headerEncoder);

        return allFixSessionsReply
            .correlationId(correlationId)
            .sessionsCount(sessionsCount);
    }

    long saveRequestAllFixSessions()
    {
        final int length = allFixSessionsReply.limit();
        return dataPublication.offer(expandableArrayBuffer, 0, length);
    }

    long saveGenericAdminReply(
        final long correlationId, final GatewayError gatewayError, final String message)
    {
        final long position = claim(GENERIC_ADMIN_REPLY_LENGTH + message.length());
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        genericAdminReply
            .wrapAndApplyHeader(buffer, offset, header)
            .correlationId(correlationId)
            .errorType(gatewayError)
            .message(message);

        bufferClaim.commit();

        return position;
    }
}
