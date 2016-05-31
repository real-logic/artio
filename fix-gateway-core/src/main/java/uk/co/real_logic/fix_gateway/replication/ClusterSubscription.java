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

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;

class ClusterSubscription extends ClusterableSubscription
{
    private final ArchiveReader archiveReader;
    private final ClusterNode node;

    private ArchiveReader.SessionReader ourArchiveReader;
    private Role role;
    private int leaderSessionId;
    private long lastAppliedPosition = DataHeaderFlyweight.HEADER_LENGTH;

    ClusterSubscription(
        final ArchiveReader archiveReader,
        final Role role,
        final ClusterNode node)
    {
        this.archiveReader = archiveReader;
        this.role = role;
        this.node = node;
    }

    void onRoleChange(final Role role, final int leaderSessionId)
    {
        this.role = role;
        this.leaderSessionId = leaderSessionId;
    }

    public int controlledPoll(final ControlledFragmentHandler handler, final int fragmentLimit)
    {
        // TODO: decide whether to flow control commit position based upon applied position
        if (validateReader())
        {
            final long commitPosition = role.commitPosition();
            final long oldAppliedPosition = this.lastAppliedPosition;
            if (commitPosition > oldAppliedPosition)
            {
                final long readUpTo = archiveReader.readUpTo(
                    leaderSessionId, oldAppliedPosition, commitPosition, handler);
                if (readUpTo != ArchiveReader.UNKNOWN_SESSION)
                {
                    this.lastAppliedPosition = readUpTo;
                }

                // TODO: return number of fragments read
                return (int) (readUpTo - oldAppliedPosition);
            }

            return 0;
        }

        return 0;
    }

    private boolean validateReader()
    {
        if (ourArchiveReader == null)
        {
            ourArchiveReader = archiveReader.session(leaderSessionId);
            if (ourArchiveReader == null)
            {
                return false;
            }
        }

        return true;
    }

    public void close()
    {
        node.close(this);
        if (ourArchiveReader != null)
        {
            ourArchiveReader.close();
        }
    }
}
