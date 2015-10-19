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

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BlockHandler;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.OK;
import static uk.co.real_logic.fix_gateway.messages.Vote.AGAINST;
import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;

public class Follower implements Role, ControlHandler, BlockHandler
{
    private static final short NO_ONE = -1;

    private final ControlSubscriber controlSubscriber = new ControlSubscriber(this);
    private final UnsafeBuffer toCommitBuffer;

    private final short id;
    private final ControlPublication controlPublication;
    private final ReplicationHandler handler;
    private final Subscription dataSubscription;
    private final Subscription controlSubscription;
    private final Replicator replicator;
    private final long replyTimeoutInMs;

    private long latestNextReceiveTimeInMs;

    private long receivedPosition;
    private long commitPosition = 0;
    private long lastAppliedPosition = 0;
    private int toCommitBufferUsed = 0;

    private short votedFor = NO_ONE;
    private int leaderShipTerm;
    private long timeInMs;

    public Follower(
        final short id,
        final ControlPublication controlPublication,
        final ReplicationHandler handler,
        final Subscription dataSubscription,
        final Subscription controlSubscription,
        final Replicator replicator,
        final long timeInMs,
        long replyTimeoutInMs,
        final int bufferSize)
    {
        this.id = id;
        this.controlPublication = controlPublication;
        this.handler = handler;
        this.dataSubscription = dataSubscription;
        this.controlSubscription = controlSubscription;
        this.replicator = replicator;
        this.replyTimeoutInMs = replyTimeoutInMs;
        updateReceiverTimeout(timeInMs);
        toCommitBuffer = new UnsafeBuffer(new byte[bufferSize]);
    }

    public int poll(final int fragmentLimit, final long timeInMs)
    {
        this.timeInMs = timeInMs;

        final int readControlMessages =
            controlSubscription.poll(controlSubscriber, fragmentLimit);

        long bytesRead = 0;
        final int bufferLimit = toCommitBuffer.capacity() - toCommitBufferUsed;
        if (bufferLimit > 0)
        {
            bytesRead = dataSubscription.blockPoll(this, bufferLimit);
            if (bytesRead > 0)
            {
                receivedPosition += bytesRead;
                controlPublication.saveMessageAcknowledgement(receivedPosition, id, OK);
                updateReceiverTimeout(timeInMs);
            }
        }

        final int committableBytes = (int) (commitPosition - lastAppliedPosition);
        if (committableBytes > 0)
        {
            handler.onBlock(toCommitBuffer, 0, committableBytes);
            lastAppliedPosition = commitPosition;

            final int uncommittedBytes = toCommitBufferUsed - committableBytes;
            if (uncommittedBytes > 0)
            {
                toCommitBuffer.putBytes(0, toCommitBuffer, committableBytes, uncommittedBytes);
            }
        }

        if (readControlMessages > 0)
        {
            updateReceiverTimeout(timeInMs);
        }

        if (timeInMs > latestNextReceiveTimeInMs)
        {
            replicator.becomeCandidate(timeInMs, leaderShipTerm, receivedPosition);
        }

        return readControlMessages + (int) bytesRead;
    }

    private void updateReceiverTimeout(final long timeInMs)
    {
        latestNextReceiveTimeInMs = timeInMs + replyTimeoutInMs;
    }

    public void onMessageAcknowledgement(
        final long newAckedPosition, final short nodeId, final AcknowledgementStatus status)
    {
        // not interested in this message
    }

    public void onRequestVote(final short candidateId, final int leaderShipTerm, final long candidatePosition)
    {
        if (canVoteFor(candidateId) && safeToVote(leaderShipTerm, candidatePosition))
        {
            //System.out.println("Voting for " + candidateId);
            votedFor = candidateId;
            controlPublication.saveReplyVote(candidateId, leaderShipTerm, FOR);
        }
        else if (candidateId != this.id)
        {
            controlPublication.saveReplyVote(candidateId, leaderShipTerm, AGAINST);
        }
    }

    private boolean safeToVote(final int leaderShipTerm, final long candidatePosition)
    {
        // Term has to be strictly greater because a follower has already
        // Voted for someone in its current leaderShipTerm and is electing for the
        // next leaderShipTerm
        return candidatePosition >= receivedPosition && leaderShipTerm > this.leaderShipTerm;
    }

    private boolean canVoteFor(final short candidateId)
    {
        return votedFor == NO_ONE || votedFor == candidateId;
    }

    public void onReplyVote(final short candidateId, final int leaderShipTerm, final Vote vote)
    {
        // not interested in this message
    }

    public void onConcensusHeartbeat(final short nodeId, final int leaderShipTerm, final long position)
    {
        if (nodeId != this.id && leaderShipTerm > this.leaderShipTerm)
        {
            follow(this.timeInMs, leaderShipTerm, position);
        }

        if (position > commitPosition)
        {
            commitPosition = position;
        }
    }

    public Follower follow(final long timeInMs, final int leaderShipTerm, final long position)
    {
        updateReceiverTimeout(timeInMs);
        votedFor = NO_ONE;
        this.leaderShipTerm = leaderShipTerm;
        this.receivedPosition = position;
        return this;
    }

    public void onBlock(final DirectBuffer srcBuffer,
                        final int offset,
                        final int length,
                        final int sessionId,
                        final int leaderShipTermId)
    {
        toCommitBuffer.putBytes(toCommitBufferUsed, srcBuffer, offset, length);
        toCommitBufferUsed += length;
    }
}
