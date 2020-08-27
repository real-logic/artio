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
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.engine.ILink3RetransmitHandler;
import uk.co.real_logic.artio.ilink.AbstractILink3Offsets;
import uk.co.real_logic.artio.ilink.AbstractILink3Parser;
import uk.co.real_logic.artio.ilink.AbstractILink3Proxy;
import uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader;
import uk.co.real_logic.artio.messages.ILinkMessageDecoder;
import uk.co.real_logic.artio.messages.ILinkMessageEncoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;

import static uk.co.real_logic.artio.LogTag.REPLAY_ATTEMPT;
import static uk.co.real_logic.artio.ilink.AbstractILink3Parser.ILINK_MESSAGE_HEADER_LENGTH;

// In ILink cases the UUID is used as a sessionId
public class ILinkReplayerSession extends ReplayerSession
{
    private final IntHashSet gapfillOnRetransmitILinkTemplateIds;
    private final ILinkMessageEncoder iLinkMessageEncoder;
    private final AbstractILink3Parser iLink3Parser;
    private final AbstractILink3Proxy iLink3Proxy;
    private final AbstractILink3Offsets iLink3Offsets;
    private final ILink3RetransmitHandler iLink3RetransmitHandler;

    private boolean mustSendSequenceMessage = false;

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
        final long sessionId,
        final Replayer replayer,
        final IntHashSet gapfillOnRetransmitILinkTemplateIds,
        final ILinkMessageEncoder iLinkMessageEncoder,
        final AbstractILink3Parser iLink3Parser,
        final AbstractILink3Proxy iLink3Proxy,
        final AbstractILink3Offsets iLink3Offsets,
        final ILink3RetransmitHandler iLink3RetransmitHandler)
    {
        super(connectionId, bufferClaim, idleStrategy, maxClaimAttempts, publication, replayQuery, beginSeqNo, endSeqNo,
            sessionId, 0, replayer);

        this.gapfillOnRetransmitILinkTemplateIds = gapfillOnRetransmitILinkTemplateIds;
        this.iLinkMessageEncoder = iLinkMessageEncoder;
        this.iLink3Parser = iLink3Parser;
        this.iLink3Proxy = iLink3Proxy;
        this.iLink3Offsets = iLink3Offsets;
        this.iLink3RetransmitHandler = iLink3RetransmitHandler;

        state = State.REPLAYING;
    }

    MessageTracker messageTracker()
    {
        return new ILink3MessageTracker(this);
    }

    public boolean attemptReplay()
    {
        switch (state)
        {
            case SEND_COMPLETE_MESSAGE:
            {
                if (mustSendSequenceMessage)
                {
                    if (sendSequence(endSeqNo + 1))
                    {
                        mustSendSequenceMessage = false;
                    }
                    else
                    {
                        return false;
                    }
                }

                return sendCompleteMessage();
            }

            case REPLAYING:
            {
                if (replayOperation.attemptReplay())
                {
                    DebugLogger.log(REPLAY_ATTEMPT, "ReplayerSession: REPLAYING step");
                    state = State.SEND_COMPLETE_MESSAGE;
                }
                return false;
            }

            default:
                return false;
        }
    }

    // Callback for replayed messages
    public Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final int encoderOffset = offset + MessageHeaderEncoder.ENCODED_LENGTH;
        final int headerOffset = encoderOffset + SimpleOpenFramingHeader.SOFH_LENGTH +
            ILinkMessageDecoder.BLOCK_LENGTH;
        final int templateId = iLink3Parser.templateId(buffer, headerOffset);
        final int blockLength = iLink3Parser.blockLength(buffer, headerOffset);
        final int version = iLink3Parser.version(buffer, headerOffset);
        final int messageOffset = headerOffset + ILINK_MESSAGE_HEADER_LENGTH;

        iLink3RetransmitHandler.onReplayedBusinessMessage(
            templateId,
            buffer,
            messageOffset,
            blockLength,
            version);

        if (gapfillOnRetransmitILinkTemplateIds.contains(templateId))
        {
            mustSendSequenceMessage = true;
            return Action.CONTINUE;
        }
        else
        {
            if (mustSendSequenceMessage)
            {
                final int seqNum = iLink3Offsets.seqNum(templateId, buffer, messageOffset);
                if (seqNum != AbstractILink3Offsets.MISSING_OFFSET)
                {
                    if (sendSequence(seqNum))
                    {
                        mustSendSequenceMessage = false;
                    }
                    else
                    {
                        return Action.ABORT;
                    }
                }
            }

            // Update connection id in case we're replaying from a previous connection.
            iLinkMessageEncoder.wrap((MutableDirectBuffer)buffer, encoderOffset);
            iLinkMessageEncoder.connection(connectionId);

            return Pressure.apply(publication.offer(buffer, offset, length));
        }
    }

    private boolean sendSequence(final int nextSentSequenceNumber)
    {
        iLink3Proxy.connectionId(connectionId);
        return !Pressure.isBackPressured(iLink3Proxy.sendSequence(sessionId, nextSentSequenceNumber));
    }

    public void close()
    {
        if (replayOperation != null)
        {
            replayOperation.close();
        }
    }
}
