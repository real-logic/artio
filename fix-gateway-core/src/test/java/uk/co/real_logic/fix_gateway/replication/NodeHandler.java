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
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.LongArrayList;
import uk.co.real_logic.fix_gateway.DebugLogger;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.LogTag.RAFT;

class NodeHandler implements ControlledFragmentHandler
{
    private final int nodeId;
    private final LongArrayList replicatedPositions = new LongArrayList();

    NodeHandler(final int nodeId)
    {
        this.nodeId = nodeId;
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final long replicatedPosition = header.position();
        DebugLogger.log(RAFT, "%d: position %d\n", nodeId, replicatedPosition);
        replicatedPositions.add(replicatedPosition);

        // Exceptions.printStackTrace();

        return CONTINUE;
    }

    void checkConsistencyOfReplicatedPositions()
    {
        // TODO
    }

    long replicatedPosition()
    {
        if (replicatedPositions.isEmpty())
        {
            return 0;
        }

        return replicatedPositions.getLong(replicatedPositions.size() - 1);
    }

}
