/*
 * Copyright 2015 Real Logic Ltd.
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

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.engine.framer.ReliefValve;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.streams.AbstractionPublication;

public class ControlPublication extends AbstractionPublication
{
    private static final int MESSAGE_ACKNOWLEDGEMENT_LENGTH = HEADER_LENGTH + MessageAcknowledgementEncoder.BLOCK_LENGTH;
    private static final int REQUEST_VOTE_LENGTH = HEADER_LENGTH + RequestVoteEncoder.BLOCK_LENGTH;
    private static final int REPLY_VOTE_LENGTH = HEADER_LENGTH + ReplyVoteEncoder.BLOCK_LENGTH;
    private static final int CONCENSUS_HEARTBEAT_LENGTH = HEADER_LENGTH + ConcensusHeartbeatEncoder.BLOCK_LENGTH;

    private final MessageAcknowledgementEncoder messageAcknowledgement = new MessageAcknowledgementEncoder();
    private final RequestVoteEncoder requestVote = new RequestVoteEncoder();
    private final ReplyVoteEncoder replyVote = new ReplyVoteEncoder();
    private final ConcensusHeartbeatEncoder concensusHeart = new ConcensusHeartbeatEncoder();

    public ControlPublication(
        final int maxClaimAttempts,
        final IdleStrategy idleStrategy,
        final AtomicCounter fails,
        final ReliefValve reliefValve,
        final Publication dataPublication)
    {
        super(maxClaimAttempts, idleStrategy, fails, reliefValve, dataPublication);
    }

    public long saveMessageAcknowledgement(final long newAckedPosition,
                                           final short nodeId,
                                           final AcknowledgementStatus status)
    {
        final long position = claim(MESSAGE_ACKNOWLEDGEMENT_LENGTH);

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

    public long saveRequestVote(final short candidateId, final long lastAckedPosition, final int term)
    {
        final long position = claim(REQUEST_VOTE_LENGTH);

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
            .lastAckedPosition(lastAckedPosition)
            .term(term);

        bufferClaim.commit();

        return position;
    }

    public long saveReplyVote(final short candidateId, final int term, final Vote vote)
    {
        final long position = claim(REPLY_VOTE_LENGTH);

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
            .candidateId(candidateId)
            .term(term)
            .vote(vote);

        bufferClaim.commit();

        return position;
    }

    public long saveConcensusHeartbeat(final short nodeId, final int term, final long position)
    {
        final long pos = claim(CONCENSUS_HEARTBEAT_LENGTH);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(concensusHeart.sbeBlockLength())
            .templateId(concensusHeart.sbeTemplateId())
            .schemaId(concensusHeart.sbeSchemaId())
            .version(concensusHeart.sbeSchemaVersion());

        offset += header.encodedLength();

        concensusHeart
            .wrap(buffer, offset)
            .nodeId(nodeId)
            .term(term)
            .position(position);

        bufferClaim.commit();

        return pos;
    }
}
