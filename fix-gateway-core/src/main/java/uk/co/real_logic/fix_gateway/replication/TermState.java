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

// TODO: hold config information for aeron streams
public class TermState
{
    private int previousLeaderSessionId;
    private int leaderSessionId;
    private int leadershipTerm;
    private long position;

    public TermState previousLeaderSessionId(int previousLeadershipSessionId)
    {
        this.previousLeaderSessionId = previousLeadershipSessionId;
        return this;
    }

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

    public TermState position(long position)
    {
        this.position = position;
        return this;
    }

    public int previousLeaderSessionId()
    {
        return previousLeaderSessionId;
    }

    public int leaderSessionId()
    {
        return leaderSessionId;
    }

    public int leadershipTerm()
    {
        return leadershipTerm;
    }

    public long position()
    {
        return position;
    }

    public String toString()
    {
        return "TermState{" +
            "previousLeaderSessionId=" + previousLeaderSessionId +
            ", leaderSessionId=" + leaderSessionId +
            ", leadershipTerm=" + leadershipTerm +
            ", position=" + position +
            '}';
    }
}
