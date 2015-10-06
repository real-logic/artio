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

/**
 * .
 */
public class Replicator implements Role
{

    private Role currentRole;

    private final Leader leader;
    private final Candidate candidate;
    private final Follower follower;

    public Replicator(
        final Leader leader,
        final Candidate candidate,
        final Follower follower)
    {
        this.leader = leader;
        this.candidate = candidate;
        this.follower = follower;

        currentRole = candidate;
    }

    public void becomeFollower(final long timeInMs, final int term, final long position)
    {
        currentRole = follower.follow(timeInMs, term, position);
    }

    public void becomeLeader(final long timeInMs, final int term)
    {
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
}
