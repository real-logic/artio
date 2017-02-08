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
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveDescriptor;

import java.util.ArrayList;
import java.util.List;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.fix_gateway.LogTag.RAFT;

class NodeHandler implements ControlledFragmentHandler
{
    private final int nodeId;
    private final List<ReplicatedMessage> replicatedMessages = new ArrayList<>();

    NodeHandler(final int nodeId)
    {
        this.nodeId = nodeId;
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final long position = header.position();
        DebugLogger.log(RAFT, "%d: position %d\n", nodeId, position);
        replicatedMessages.add(new ReplicatedMessage(position, length));

        // Exceptions.printStackTrace();

        return CONTINUE;
    }

    void checkConsistencyOfReplicatedPositions()
    {
        int position = 0;
        for (ReplicatedMessage replicatedMessage : replicatedMessages)
        {
            int messageLength = DataHeaderFlyweight.HEADER_LENGTH + replicatedMessage.length;
            position = ArchiveDescriptor.alignTerm(position + messageLength);
            assertEquals(replicatedMessages.toString(), position, replicatedMessage.position);
        }
    }

    private static final class ReplicatedMessage
    {
        private final long position;
        private final int length;

        private ReplicatedMessage(final long position, final int length)
        {
            this.position = position;
            this.length = length;
        }

        public String toString()
        {
            return "{" +
                "position=" + position +
                ", length=" + length +
                '}';
        }
    }

    long replicatedPosition()
    {
        if (replicatedMessages.isEmpty())
        {
            return 0;
        }

        final ReplicatedMessage lastMessage = replicatedMessages.get(replicatedMessages.size() - 1);
        return lastMessage.position;
    }

}
