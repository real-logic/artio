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
package uk.co.real_logic.fix_gateway.engine.logger;

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.CollectionUtil;
import org.agrona.concurrent.Agent;
import uk.co.real_logic.fix_gateway.engine.CompletionPosition;
import uk.co.real_logic.fix_gateway.replication.ClusterableSubscription;
import uk.co.real_logic.fix_gateway.replication.ReservedValue;

import java.util.List;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.fix_gateway.engine.logger.ArchiveDescriptor.alignTerm;

/**
 * Incrementally builds indexes by polling a subscription.
 */
public class Indexer implements Agent, ControlledFragmentHandler
{
    private static final int LIMIT = 10;

    private final List<Index> indices;
    private final ArchiveReader archiveReader;
    private final ClusterableSubscription subscription;
    private final String agentNamePrefix;
    private final CompletionPosition completionPosition;

    public Indexer(
        final List<Index> indices,
        final ArchiveReader archiveReader,
        final ClusterableSubscription subscription,
        final String agentNamePrefix,
        final CompletionPosition completionPosition)
    {
        this.indices = indices;
        this.archiveReader = archiveReader;
        this.subscription = subscription;
        this.agentNamePrefix = agentNamePrefix;
        this.completionPosition = completionPosition;
        catchIndexUp();
    }

    public int doWork() throws Exception
    {
        return subscription.controlledPoll(this, LIMIT) + CollectionUtil.sum(indices, Index::doWork);
    }

    private void catchIndexUp()
    {
        for (final Index index : indices)
        {
            index.readLastPosition((aeronSessionId, endOfLastMessageposition) ->
            {
                final ArchiveReader.SessionReader sessionReader = archiveReader.session(aeronSessionId);
                if (sessionReader != null)
                {
                    do
                    {

                        final long nextMessagePosition = alignTerm(endOfLastMessageposition) + HEADER_LENGTH;
                        endOfLastMessageposition = sessionReader.read(nextMessagePosition, index);
                    }
                    while (endOfLastMessageposition > 0);
                }
            });
        }
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final int streamId = ReservedValue.streamId(header);
        final int aeronSessionId = header.sessionId();
        final long position = header.position();
        for (final Index index : indices)
        {
            index.indexRecord(buffer, offset, length, streamId, aeronSessionId, position);
        }

        return CONTINUE;
    }

    public void onClose()
    {
        quiesce();

        indices.forEach(Index::close);
        archiveReader.close();
        subscription.close();
    }

    private void quiesce()
    {
        while (!completionPosition.hasCompleted())
        {
            Thread.yield();
        }

        if (completionPosition.wasStartupComplete())
        {
            return;
        }

        // We know that any remaining data to quiesce at this point must be in the subscription.
        subscription.controlledPoll(this::quiesceFragment, Integer.MAX_VALUE);
    }

    private Action quiesceFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (completedPosition(header.sessionId()) <= header.position())
        {
            return onFragment(buffer, offset, length, header);
        }

        return CONTINUE;
    }

    private long completedPosition(final int aeronSessionId)
    {
        return completionPosition.positions().get(aeronSessionId);
    }

    public String roleName()
    {
        return agentNamePrefix + "Indexer";
    }
}
