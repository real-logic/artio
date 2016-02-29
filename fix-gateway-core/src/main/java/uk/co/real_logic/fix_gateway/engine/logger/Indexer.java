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
package uk.co.real_logic.fix_gateway.engine.logger;

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.Agent;

import java.util.List;

// TODO: add ability to late-join and catchup with indexing
public class Indexer implements Agent, FragmentHandler
{
    private static final int LIMIT = 10;

    private final List<Index> indices;
    private final Subscription subscription;

    public Indexer(
        final List<Index> indices, final Subscription subscription)
    {
        this.indices = indices;
        this.subscription = subscription;
    }

    public int doWork() throws Exception
    {
        return subscription.poll(this, LIMIT);
    }

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final int streamId = header.streamId();
        final int aeronSessionId = header.sessionId();
        final long position = header.position();
        for (final Index index : indices)
        {
            index.indexRecord(buffer, offset, length, streamId, aeronSessionId, position);
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
