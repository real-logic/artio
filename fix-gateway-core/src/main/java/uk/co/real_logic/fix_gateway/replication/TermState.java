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

public class TermState
{
    /** the aeron session id of the current leader */
    private int leaderSessionId;

    /** The raft leader's current term number */
    private int leadershipTerm;

    /** The position within the current leadership term that we have read data on the session up to. */
    private long receivedPosition;

    /** The position within the current leadership term that we have applied to our state machine. */
    private long lastAppliedPosition;

    /** The position within the current leadership term that we can commit up to. */
    private long commitPosition;

    public TermState leaderSessionId(int leadershipSessionId)
    {
        this.leaderSessionId = leadershipSessionId;
        return this;
    }

    public TermState leadershipTerm(int leadershipTerm)
    {
        this.leadershipTerm = leadershipTerm;
        return this;
    }

    public TermState receivedPosition(final long receivedPosition)
    {
        this.receivedPosition = receivedPosition;
        return this;
    }

    public TermState lastAppliedPosition(final long lastAppliedPosition)
    {
        this.lastAppliedPosition = lastAppliedPosition;
        return this;
    }

    public TermState commitPosition(long commitPosition)
    {
        this.commitPosition = commitPosition;
        return this;
    }

    public TermState allPositions(long position)
    {
        receivedPosition(position);
        lastAppliedPosition(position);
        commitPosition(position);
        return this;
    }

    public int leaderSessionId()
    {
        return leaderSessionId;
    }

    public int leadershipTerm()
    {
        return leadershipTerm;
    }

    public long receivedPosition()
    {
        return receivedPosition;
    }

    public long lastAppliedPosition()
    {
        return lastAppliedPosition;
    }

    public long commitPosition()
    {
        return commitPosition;
    }

    public String toString()
    {
        return "TermState{" +
            ", leaderSessionId=" + leaderSessionId +
            ", leadershipTerm=" + leadershipTerm +
            ", receivedPosition=" + receivedPosition +
            ", lastAppliedPosition=" + lastAppliedPosition +
            ", commitPosition=" + commitPosition +
            '}';
    }
}
