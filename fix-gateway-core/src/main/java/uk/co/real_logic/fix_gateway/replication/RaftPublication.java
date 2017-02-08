/*
 * Copyright 2015-2016 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.fix_gateway.replication;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.replication.messages.*;

import static uk.co.real_logic.fix_gateway.replication.messages.ReplyVoteEncoder.nodeStateHeaderLength;

// NB: doens't extend ClaimablePublication because it works on raw Publication objects, not clusterable publications
class RaftPublication
{
    private static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final int MESSAGE_ACKNOWLEDGEMENT_LENGTH =
        HEADER_LENGTH + MessageAcknowledgementEncoder.BLOCK_LENGTH;
    private static final int REQUEST_VOTE_LENGTH = HEADER_LENGTH + RequestVoteEncoder.BLOCK_LENGTH;
    private static final int REPLY_VOTE_LENGTH =
        HEADER_LENGTH + ReplyVoteEncoder.BLOCK_LENGTH + nodeStateHeaderLength();
    private static final int CONSENSUS_HEARTBEAT_LENGTH = HEADER_LENGTH + ConsensusHeartbeatEncoder.BLOCK_LENGTH;
    private static final int RESEND_BLOCK_LENGTH =
        HEADER_LENGTH + ResendEncoder.BLOCK_LENGTH + ResendDecoder.bodyHeaderLength();

    protected final MessageHeaderEncoder header = new MessageHeaderEncoder();

    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageAcknowledgementEncoder messageAcknowledgement = new MessageAcknowledgementEncoder();
    private final RequestVoteEncoder requestVote = new RequestVoteEncoder();
    private final ReplyVoteEncoder replyVote = new ReplyVoteEncoder();
    private final ConsensusHeartbeatEncoder consensusHeart = new ConsensusHeartbeatEncoder();
    private final ResendEncoder resend = new ResendEncoder();

    private final long maxClaimAttempts;
    private final Publication dataPublication;
    private final IdleStrategy idleStrategy;
    private final AtomicCounter fails;

    RaftPublication(
        final int maxClaimAttempts,
        final IdleStrategy idleStrategy,
        final AtomicCounter fails,
        final Publication dataPublication)
    {
        this.maxClaimAttempts = maxClaimAttempts;
        this.idleStrategy = idleStrategy;
        this.fails = fails;
        this.dataPublication = dataPublication;
    }

    long saveMessageAcknowledgement(
        final long newAckedPosition,
        final short nodeId,
        final AcknowledgementStatus status)
    {
        final long position = claim(MESSAGE_ACKNOWLEDGEMENT_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(messageAcknowledgement.sbeBlockLength())
            .templateId(messageAcknowledgement.sbeTemplateId())
            .schemaId(messageAcknowledgement.sbeSchemaId())
            .version(messageAcknowledgement.sbeSchemaVersion());

        offset += header.encodedLength();

        messageAcknowledgement
            .wrap(buffer, offset)
            .newAckedPosition(newAckedPosition)
            .nodeId(nodeId)
            .status(status);

        bufferClaim.commit();

        return position;
    }

    long saveRequestVote(
        final short candidateId, final int candidateSessionId, final long lastAckedPosition, final int leaderShipTerm)
    {
        final long position = claim(REQUEST_VOTE_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(requestVote.sbeBlockLength())
            .templateId(requestVote.sbeTemplateId())
            .schemaId(requestVote.sbeSchemaId())
            .version(requestVote.sbeSchemaVersion());

        offset += header.encodedLength();

        requestVote
            .wrap(buffer, offset)
            .candidateId(candidateId)
            .candidateSessionId(candidateSessionId)
            .lastAckedPosition(lastAckedPosition)
            .leaderShipTerm(leaderShipTerm);

        bufferClaim.commit();

        return position;
    }

    long saveReplyVote(
        final short senderNodeId,
        final short candidateId,
        final int leaderShipTerm,
        final Vote vote,
        final DirectBuffer nodeState)
    {
        final int nodeStateLength = nodeState.capacity();
        final long position = claim(REPLY_VOTE_LENGTH + nodeStateLength);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(replyVote.sbeBlockLength())
            .templateId(replyVote.sbeTemplateId())
            .schemaId(replyVote.sbeSchemaId())
            .version(replyVote.sbeSchemaVersion());

        offset += header.encodedLength();

        replyVote
            .wrap(buffer, offset)
            .senderNodeId(senderNodeId)
            .candidateId(candidateId)
            .leaderShipTerm(leaderShipTerm)
            .vote(vote)
            .putNodeState(nodeState, 0, nodeStateLength);

        bufferClaim.commit();

        return position;
    }

    long saveConsensusHeartbeat(
        final short nodeId,
        final int leaderShipTerm,
        final long position,
        final int leaderSessionId,
        final long streamStartPosition,
        final long streamPosition)
    {
        final long pos = claim(CONSENSUS_HEARTBEAT_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(consensusHeart.sbeBlockLength())
            .templateId(consensusHeart.sbeTemplateId())
            .schemaId(consensusHeart.sbeSchemaId())
            .version(consensusHeart.sbeSchemaVersion());

        offset += header.encodedLength();

        consensusHeart
            .wrap(buffer, offset)
            .nodeId(nodeId)
            .leaderShipTerm(leaderShipTerm)
            .position(position)
            .leaderSessionId(leaderSessionId)
            .streamStartPosition(streamStartPosition)
            .streamPosition(streamPosition);

        bufferClaim.commit();

        return pos;
    }

    long saveResend(
        final int leaderSessionId,
        final int leaderShipTerm,
        final long startPosition,
        final long streamStartPosition,
        final DirectBuffer bodyBuffer,
        final int bodyOffset,
        final int bodyLength)
    {
        final long position = claim(RESEND_BLOCK_LENGTH + bodyLength);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(resend.sbeBlockLength())
            .templateId(resend.sbeTemplateId())
            .schemaId(resend.sbeSchemaId())
            .version(resend.sbeSchemaVersion());

        offset += header.encodedLength();

        resend
            .wrap(buffer, offset)
            .leaderSessionId(leaderSessionId)
            .leaderShipTerm(leaderShipTerm)
            .startPosition(startPosition)
            .streamStartPosition(streamStartPosition)
            .putBody(bodyBuffer, bodyOffset, bodyLength);

        bufferClaim.commit();

        return position;
    }

    private long claim(final int framedLength)
    {
        long position;
        long i = 0;
        do
        {
            position = dataPublication.tryClaim(framedLength, bufferClaim);

            if (position > 0L)
            {
                return position;
            }
            else
            {
                idleStrategy.idle();
            }

            fails.increment();
            i++;
        }
        while (i <= maxClaimAttempts);

        idleStrategy.reset();

        return position;
    }

    public void close()
    {
        dataPublication.close();
    }
}
