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

public class Follower implements Role, RaftHandler, BlockHandler
{
    private static final short NO_ONE = -1;

    private final RaftSubscriber raftSubscriber = new RaftSubscriber(this);
    private final UnsafeBuffer toCommitBuffer;

    private final short nodeId;
    private final ReplicationHandler handler;
    private final RaftNode raftNode;
    private final long replyTimeoutInMs;
    private final TermState termState;

    private RaftPublication acknowledgementPublication;
    private RaftPublication controlPublication;
    private Subscription dataSubscription;
    private Subscription controlSubscription;
    private long receivedPosition;
    private long commitPosition = 0;
    private long lastAppliedPosition = 0;
    private int toCommitBufferUsed = 0;
    private long latestNextReceiveTimeInMs;
    private short votedFor = NO_ONE;
    private int leaderShipTerm;
    private long timeInMs;

    public Follower(
        final short nodeId,
        final ReplicationHandler handler,
        final RaftNode raftNode,
        final long timeInMs,
        long replyTimeoutInMs,
        final int bufferSize,
        final TermState termState)
    {
        this.nodeId = nodeId;
        this.handler = handler;
        this.raftNode = raftNode;
        this.replyTimeoutInMs = replyTimeoutInMs;
        this.termState = termState;
        updateReceiverTimeout(timeInMs);
        toCommitBuffer = new UnsafeBuffer(new byte[bufferSize]);
    }

    public int poll(final int fragmentLimit, final long timeInMs)
    {
        this.timeInMs = timeInMs;

        final int readControlMessages =
            controlSubscription.poll(raftSubscriber, fragmentLimit);

        long bytesRead = 0;
        final int bufferLimit = toCommitBuffer.capacity() - toCommitBufferUsed;
        if (bufferLimit > 0)
        {
            bytesRead = dataSubscription.blockPoll(this, bufferLimit);
            if (bytesRead > 0)
            {
                receivedPosition += bytesRead;
                acknowledgementPublication.saveMessageAcknowledgement(receivedPosition, nodeId, OK);
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
            raftNode.transitionToCandidate(timeInMs);
        }

        return readControlMessages + (int) bytesRead;
    }

    public void closeStreams()
    {
        controlPublication.close();
        acknowledgementPublication.close();
        controlSubscription.close();
        dataSubscription.close();
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
            controlPublication.saveReplyVote(nodeId, candidateId, leaderShipTerm, FOR);
        }
        else if (candidateId != nodeId)
        {
            controlPublication.saveReplyVote(nodeId, candidateId, leaderShipTerm, AGAINST);
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

    public void onReplyVote(
        final short senderNodeId, final short candidateId, final int leaderShipTerm, final Vote vote)
    {
        // not interested in this message
    }

    public void onConcensusHeartbeat(final short leaderNodeId,
                                     final int leaderShipTerm,
                                     final long position,
                                     final int leaderSessionId)
    {
        if (leaderNodeId != this.nodeId && leaderShipTerm > this.leaderShipTerm)
        {
            termState
                .leadershipTerm(leaderShipTerm)
                .position(position)
                .leaderSessionId(leaderSessionId);

            follow(this.timeInMs);
        }

        if (position > commitPosition)
        {
            commitPosition = position;
        }
    }

    public Follower follow(final long timeInMs)
    {
        updateReceiverTimeout(timeInMs);
        votedFor = NO_ONE;
        this.leaderShipTerm = termState.leadershipTerm();
        this.receivedPosition = termState.position();
        return this;
    }

    public void onBlock(final DirectBuffer srcBuffer,
                        final int offset,
                        final int length,
                        final int sessionId,
                        final int termId)
    {
        if (sessionId == termState.leaderSessionId())
        {
            toCommitBuffer.putBytes(toCommitBufferUsed, srcBuffer, offset, length);
            toCommitBufferUsed += length;
        }
    }

    public Follower acknowledgementPublication(final RaftPublication acknowledgementPublication)
    {
        this.acknowledgementPublication = acknowledgementPublication;
        return this;
    }

    public Follower controlPublication(final RaftPublication controlPublication)
    {
        this.controlPublication = controlPublication;
        return this;
    }

    public Follower dataSubscription(final Subscription dataSubscription)
    {
        this.dataSubscription = dataSubscription;
        return this;
    }

    public Follower controlSubscription(final Subscription controlSubscription)
    {
        this.controlSubscription = controlSubscription;
        return this;
    }
}
