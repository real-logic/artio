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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.messages.ILinkMessageEncoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;

import static uk.co.real_logic.artio.LogTag.REPLAY_ATTEMPT;

// In ILink cases the UUID is used as a sessionId
public class ILinkReplayerSession extends ReplayerSession
{
    private static final ILinkMessageEncoder ILINK_MESSAGE_ENCODER = new ILinkMessageEncoder();

    private enum State
    {
        REPLAYING,
        SEND_COMPLETE_MESSAGE
    }

    private State state;

    public ILinkReplayerSession(
        final long connectionId,
        final BufferClaim bufferClaim,
        final IdleStrategy idleStrategy,
        final int maxClaimAttempts,
        final ExclusivePublication publication,
        final ReplayQuery replayQuery,
        final int beginSeqNo,
        final int endSeqNo,
        final long sessionId)
    {
        super(connectionId, bufferClaim, idleStrategy, maxClaimAttempts, publication, replayQuery, beginSeqNo, endSeqNo,
            sessionId, 0);

        state = State.REPLAYING;
    }

    MessageTracker messageTracker()
    {
        return new ILink3MessageTracker(this);
    }

    public boolean attempReplay()
    {
        switch (state)
        {
            case REPLAYING:
            {
                if (replayOperation.attemptReplay())
                {
                    DebugLogger.log(REPLAY_ATTEMPT, "ReplayerSession: REPLAYING step");
                    state = State.SEND_COMPLETE_MESSAGE;
                }
                return false;
            }

            case SEND_COMPLETE_MESSAGE:
            {
                return sendCompleteMessage();
            }

            default:
                return false;
        }
    }

    // Callback for replayed messages
    public Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        // Update connection id in case we're replaying from a previous connection.
        ILINK_MESSAGE_ENCODER.wrap((MutableDirectBuffer)buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH);
        ILINK_MESSAGE_ENCODER.connection(connectionId);

        return Pressure.apply(publication.offer(buffer, offset, length));
    }

    public void close()
    {
        if (replayOperation != null)
        {
            replayOperation.close();
        }
    }
}
