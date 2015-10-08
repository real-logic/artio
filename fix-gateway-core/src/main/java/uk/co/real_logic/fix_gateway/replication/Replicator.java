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
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BlockHandler;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.fix_gateway.DebugLogger;

/**
 * .
 */
public class Replicator implements Role
{
    public static final long NOT_LEADER = -3;

    private final short nodeId;
    private final Publication dataPublication;
    private Role currentRole;

    private final Leader leader;
    private final Candidate candidate;
    private final Follower follower;

    public Replicator(
        final short nodeId,
        final ControlPublication controlPublication,
        final Publication dataPublication,
        final Subscription controlSubscription,
        final Subscription dataSubscription,
        final IntHashSet otherNodes,
        final long timeInMs,
        final long timeoutIntervalInMs,
        final TermAcknowledgementStrategy termAcknowledgementStrategy,
        final BlockHandler handler)
    {
        this.nodeId = nodeId;
        this.dataPublication = dataPublication;

        final long heartbeatTimeInMs = timeoutIntervalInMs / 2;

        leader = new Leader(
            nodeId,
            termAcknowledgementStrategy,
            otherNodes,
            controlPublication,
            controlSubscription,
            dataSubscription,
            this,
            handler,
            timeInMs,
            heartbeatTimeInMs);

        candidate = new Candidate(
            nodeId,
            controlPublication,
            controlSubscription,
            this,
            otherNodes.size() + 1,
            timeoutIntervalInMs);

        follower = new Follower(
            nodeId,
            controlPublication,
            (buffer, offset, length, header) -> System.out.println("TODO"),
            dataSubscription,
            controlSubscription,
            this,
            timeInMs,
            timeoutIntervalInMs);

        currentRole = follower;
    }

    public void becomeFollower(final long timeInMs, final int term, final long position)
    {
        DebugLogger.log("%d: Follower @ %d in %d\n", nodeId, timeInMs, term);

        currentRole = follower.follow(timeInMs, term, position);
    }

    public void becomeLeader(final long timeInMs, final int term)
    {
        DebugLogger.log("%d: Leader @ %d in %d\n", nodeId, timeInMs, term);

        currentRole = leader.getsElected(timeInMs, term);
    }

    public void becomeCandidate(final long timeInMs, final int oldTerm, final long position)
    {
        currentRole = candidate;
        candidate.startNewElection(timeInMs, oldTerm, position);
    }

    public int poll(final int fragmentLimit, final long timeInMs)
    {
        return currentRole.poll(fragmentLimit, timeInMs);
    }

    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        if (!isLeader())
        {
            return NOT_LEADER;
        }

        return dataPublication.offer(buffer, offset, length);
    }

    public boolean isLeader()
    {
        return currentRole == leader;
    }

    public short nodeId()
    {
        return nodeId;
    }
}
