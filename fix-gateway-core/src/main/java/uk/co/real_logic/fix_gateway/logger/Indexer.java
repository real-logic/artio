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
package uk.co.real_logic.fix_gateway.logger;

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.fix_gateway.replication.ReplicationStreams;

import java.util.List;

// TODO: add ability to late-join and catchup with indexing
public class Indexer implements Agent, DataHandler
{
    private static final int LIMIT = 10;

    private final List<Index> indices;
    private final Subscription subscription;

    public Indexer(final List<Index> indices, final ReplicationStreams streams)
    {
        this.indices = indices;
        this.subscription = streams.dataSubscription(this);
    }

    public int doWork() throws Exception
    {
        return subscription.poll(LIMIT);
    }

    @Override
    public void onData(DirectBuffer buffer, int offset, int length, Header header)
    {
        for (final Index index : indices)
        {
            index.indexRecord(buffer, offset, length, header.streamId());
        }
    }

    public void onClose()
    {
        indices.forEach(Index::close);
    }

    public String roleName()
    {
        return "Indexer";
    }
}
