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

import io.aeron.BufferBuilder;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;

import java.util.function.IntFunction;

import static io.aeron.BufferBuilder.INITIAL_CAPACITY;
import static io.aeron.logbuffer.FrameDescriptor.BEGIN_FRAG_FLAG;
import static io.aeron.logbuffer.FrameDescriptor.END_FRAG_FLAG;
import static io.aeron.logbuffer.FrameDescriptor.UNFRAGMENTED;

/**
 * Equivalent of the {@link io.aeron.ControlledFragmentAssembler} for cluster subscriptions.
 */
public class ClusterFragmentAssembler implements ClusterFragmentHandler
{
    private final ClusterFragmentHandler delegate;
    private final Int2ObjectHashMap<BufferBuilder> builderBySessionIdMap = new Int2ObjectHashMap<>();
    private final IntFunction<BufferBuilder> builderFunc;

    public ClusterFragmentAssembler(final ClusterFragmentHandler delegate)
    {
        this.delegate = delegate;
        this.builderFunc = ignore -> new BufferBuilder(INITIAL_CAPACITY);
    }

    public Action onFragment(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ClusterHeader header)
    {
        final byte flags = header.flags();

        Action action = Action.CONTINUE;

        if ((flags & UNFRAGMENTED) == UNFRAGMENTED)
        {
            action = delegate.onFragment(buffer, offset, length, header);
        }
        else
        {
            if ((flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG)
            {
                final BufferBuilder builder = builderBySessionIdMap.computeIfAbsent(header.sessionId(), builderFunc);
                builder.reset().append(buffer, offset, length);
            }
            else
            {
                final BufferBuilder builder = builderBySessionIdMap.get(header.sessionId());
                if (null != builder && builder.limit() != 0)
                {
                    final int limit = builder.limit();
                    builder.append(buffer, offset, length);

                    if ((flags & END_FRAG_FLAG) == END_FRAG_FLAG)
                    {
                        final int msgLength = builder.limit();
                        action = delegate.onFragment(builder.buffer(), 0, msgLength, header);

                        if (Action.ABORT == action)
                        {
                            builder.limit(limit);
                        }
                        else
                        {
                            builder.reset();
                        }
                    }
                }
            }
        }

        return action;
    }
}
