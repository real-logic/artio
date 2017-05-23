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
package uk.co.real_logic.fix_gateway.replication;

import io.aeron.Subscription;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;

import java.util.concurrent.atomic.AtomicInteger;

class RaftArchiver
{
    private final AtomicInteger leaderSessionId;
    private final Archiver archiver;

    private Archiver.SessionArchiver leaderArchiver;

    RaftArchiver(final AtomicInteger leaderSessionId, final Archiver archiver)
    {
        this.leaderSessionId = leaderSessionId;
        this.archiver = archiver;
    }

    boolean checkLeaderArchiver()
    {
        // Leader may not have written anything onto its data stream when it becomes the leader
        // Most of the time this will be false
        if (leaderArchiver == null)
        {
            leaderArchiver = archiver.session(leaderSessionId());
            if (leaderArchiver == null)
            {
                return true;
            }
        }

        return false;
    }

    private int leaderSessionId()
    {
        return leaderSessionId.get();
    }

    long archivedPosition()
    {
        if (leaderArchiver != null)
        {
            return leaderArchiver.archivedPosition();
        }
        else
        {
            return 0;
        }
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
        leaderArchiver = archiver.session(leaderSessionId());
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
