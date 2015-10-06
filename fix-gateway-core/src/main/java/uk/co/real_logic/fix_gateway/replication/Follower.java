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
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.MISSING_LOG_ENTRIES;
import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.OK;
import static uk.co.real_logic.fix_gateway.messages.Vote.AGAINST;
import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;

public class Follower implements Role, FragmentHandler, ControlHandler
{
    private static final short NO_ONE = -1;

    private final ControlSubscriber controlSubscriber = new ControlSubscriber(this);

    private final short id;
    private final ControlPublication controlPublication;
    private final FragmentHandler delegate;
    private final Subscription dataSubscription;
    private final Subscription controlSubscription;
    private final Replicator replicator;
    private final long replyTimeoutInMs;

    private long latestNextReceiveTimeInMs;
    private long oldPosition = 0;
    private long position;
    private boolean receivedHeartbeat = false;

    private short votedFor = NO_ONE;
    private int term;

    public Follower(
        final short id,
        final ControlPublication controlPublication,
        final FragmentHandler delegate,
        final Subscription dataSubscription,
        final Subscription controlSubscription,
        final Replicator replicator,
        final long timeInMs,
        long replyTimeoutInMs)
    {
        this.id = id;
        this.controlPublication = controlPublication;
        this.delegate = delegate;
        this.dataSubscription = dataSubscription;
        this.controlSubscription = controlSubscription;
        this.replicator = replicator;
        this.replyTimeoutInMs = replyTimeoutInMs;
        this.latestNextReceiveTimeInMs = timeInMs + replyTimeoutInMs;
    }

    public int poll(final int fragmentLimit, final long timeInMs)
    {
        final int readControlMessages =
            controlSubscription.poll(controlSubscriber, fragmentLimit);

        final int readDataMessages = dataSubscription.poll(this, fragmentLimit);

        if (position > oldPosition)
        {
            controlPublication.saveMessageAcknowledgement(position, id, OK);
            onReceivedMessage(timeInMs);
            oldPosition = position;
        }

        if (receivedHeartbeat)
        {
            onReceivedMessage(timeInMs);
            receivedHeartbeat = false;
        }

        if (timeInMs > latestNextReceiveTimeInMs)
        {
            replicator.becomeCandidate(timeInMs, term, position);
        }

        return readControlMessages + readDataMessages;
    }

    private void onReceivedMessage(final long timeInMs)
    {
        latestNextReceiveTimeInMs = timeInMs + replyTimeoutInMs;
    }

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final int headerOffset = header.offset();
        if (position == headerOffset)
        {
            position = header.position();
        }
        else if (position < headerOffset)
        {
            controlPublication.saveMessageAcknowledgement(position, id, MISSING_LOG_ENTRIES);
        }
    }

    public void onMessageAcknowledgement(
        final long newAckedPosition, final short nodeId, final AcknowledgementStatus status)
    {
        // not interested in this message
    }

    public void onRequestVote(final short candidateId, final int term, final long candidatePosition)
    {
        if (canVoteFor(candidateId) && safeToVote(term, candidatePosition))
        {
            //System.out.println("Voting for " + candidateId);
            votedFor = candidateId;
            controlPublication.saveReplyVote(candidateId, term, FOR);
        }
        else if (candidateId != this.id)
        {
            controlPublication.saveReplyVote(candidateId, term, AGAINST);
        }
    }

    private boolean safeToVote(final int term, final long candidatePosition)
    {
        // Term has to be strictly greater because a follower has already
        // Voted for someone in its current term and is electing for the
        // next term
        return candidatePosition >= position && term > this.term;
    }

    private boolean canVoteFor(final short candidateId)
    {
        return votedFor == NO_ONE || votedFor == candidateId;
    }

    public void onReplyVote(final short candidateId, final int term, final Vote vote)
    {
        // not interested in this message
    }

    public void onConcensusHeartbeat(final short nodeId, final int term, final long position)
    {
        receivedHeartbeat = true;
    }

    public Follower follow(final int term, final long position)
    {
        votedFor = NO_ONE;
        this.term = term;
        this.position = position;
        return this;
    }
}
