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

import io.aeron.Subscription;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;

// TODO: extract common archiver functionality from leader and follower
public class RaftArchiver
{

    private final Archiver archiver;
    private final TermState termState;

    private Archiver.SessionArchiver leaderArchiver;

    public RaftArchiver(final Archiver archiver, final TermState termState)
    {
        this.archiver = archiver;
        this.termState = termState;
    }

    boolean checkLeaderArchiver()
    {
        // Leader may not have written anything onto its data stream when it becomes the leader
        // Most of the time this will be false
        if (leaderArchiver == null)
        {
            leaderArchiver = archiver.session(termState.leaderSessionId());
            termState.leaderSessionId(termState.leaderSessionId());
            if (leaderArchiver == null)
            {
                return true;
            }
        }
        return false;
    }

    long archivedPosition()
    {
        return leaderArchiver.archivedPosition();
    }

    int poll()
    {
        return leaderArchiver.poll();
    }

    void patch(final DirectBuffer bodyBuffer, final int bodyOffset, final int bodyLength)
    {
        leaderArchiver.patch(bodyBuffer, bodyOffset, bodyLength);
    }

    void onLeader()
    {
        final int sessionId = termState.leaderSessionId();
        leaderArchiver = archiver.session(sessionId);
    }

    void onNoLeader()
    {
        leaderArchiver = null;
    }

    void dataSubscription(final Subscription dataSubscription)
    {
        archiver.subscription(dataSubscription);
    }
}
