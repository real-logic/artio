/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.replication;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class TermState
{
    private static final int NO_LEADER = 0;

    /** the aeron session id of the current leader */
    private AtomicInteger leaderSessionId = new AtomicInteger(NO_LEADER);

    /** The raft leader's current term number */
    private int leadershipTerm;

    /** The position that we have read data on the session up to. */
    private long receivedPosition;

    /** The position that we have applied to our state machine. */
    private long lastAppliedPosition;

    /** The position that we can commit up to. */
    private final AtomicLong consensusPosition = new AtomicLong(0);

    // replicated - transport = delta
    private long transportPositionDelta;

    TermState leaderSessionId(final int leadershipSessionId)
    {
        this.leaderSessionId.set(leadershipSessionId);
        return this;
    }

    TermState noLeader()
    {
        leaderSessionId.set(NO_LEADER);
        return this;
    }

    TermState leadershipTerm(final int leadershipTerm)
    {
        this.leadershipTerm = leadershipTerm;
        return this;
    }

    TermState receivedPosition(final long receivedPosition)
    {
        this.receivedPosition = receivedPosition;
        return this;
    }

    TermState lastAppliedPosition(final long lastAppliedPosition)
    {
        this.lastAppliedPosition = lastAppliedPosition;
        return this;
    }

    TermState consensusPosition(final long consensusPosition)
    {
        consensusPosition().set(consensusPosition);
        return this;
    }

    TermState allPositions(final long position)
    {
        receivedPosition(position);
        lastAppliedPosition(position);
        consensusPosition(position);
        return this;
    }

    AtomicInteger leaderSessionId()
    {
        return leaderSessionId;
    }

    boolean hasLeader()
    {
        return leaderSessionId.get() != NO_LEADER;
    }

    int leadershipTerm()
    {
        return leadershipTerm;
    }

    void incLeadershipTerm()
    {
        leadershipTerm++;
    }

    long receivedPosition()
    {
        return receivedPosition;
    }

    void moveReceivedPosition(final long by)
    {
        receivedPosition += by;
    }

    long lastAppliedPosition()
    {
        return lastAppliedPosition;
    }

    AtomicLong consensusPosition()
    {
        return consensusPosition;
    }

    TermState reset()
    {
        noLeader();
        leaderSessionId(0);
        leadershipTerm(0);
        allPositions(0);
        return this;
    }

    public String toString()
    {
        return "TermState{" +
            "leaderSessionId=" + leaderSessionId +
            ", leadershipTerm=" + leadershipTerm +
            ", receivedPosition=" + receivedPosition +
            ", lastAppliedPosition=" + lastAppliedPosition +
            ", consensusPosition=" + consensusPosition +
            '}';
    }

    /** Must be written before the leader session id is set when you become leader */
    TermState transportPositionDelta(final long transportPositionDelta)
    {
        this.transportPositionDelta = transportPositionDelta;
        return this;
    }

    /** Can only be read after the leader session id has been read HB */
    long transportPositionDelta()
    {
        return transportPositionDelta;
    }
}
