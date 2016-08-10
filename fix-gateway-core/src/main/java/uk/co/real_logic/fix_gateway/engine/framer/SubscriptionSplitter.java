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
package uk.co.real_logic.fix_gateway.engine.framer;

import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.LongHashSet;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.EngineDescriptorStore;
import uk.co.real_logic.fix_gateway.messages.DisconnectDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.protocol.EngineProtocolSubscription;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.replication.ClusterablePublication;
import uk.co.real_logic.fix_gateway.replication.ClusterableStreams;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

/**
 * Splits the subscription out into messages that we deal with locally vs cluster
 */
class SubscriptionSplitter implements ControlledFragmentHandler
{
    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final DisconnectDecoder disconnect = new DisconnectDecoder();

    private final ClusterableStreams clusterableStreams;
    private final EngineProtocolSubscription engineProtocolSubscription;
    private final ClusterablePublication clusterPublication;
    private final GatewayPublication replyPublication;
    private final EngineDescriptorStore engineDescriptorStore;
    private final String bindAddress;
    private final LongHashSet replicatedConnectionIds;

    SubscriptionSplitter(
        final ClusterableStreams clusterableStreams,
        final EngineProtocolSubscription engineProtocolSubscription,
        final ClusterablePublication clusterPublication,
        final GatewayPublication replyPublication,
        final EngineDescriptorStore engineDescriptorStore,
        final String bindAddress,
        final LongHashSet replicatedConnectionIds)
    {
        this.clusterableStreams = clusterableStreams;
        this.engineProtocolSubscription = engineProtocolSubscription;
        this.clusterPublication = clusterPublication;
        this.replyPublication = replyPublication;
        this.engineDescriptorStore = engineDescriptorStore;
        this.bindAddress = bindAddress;
        this.replicatedConnectionIds = replicatedConnectionIds;
    }

    public Action onFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        if (clusterableStreams.isLeader())
        {
            messageHeader.wrap(buffer, offset);
            final int messageOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;

            switch (messageHeader.templateId())
            {
                case FixMessageDecoder.TEMPLATE_ID:
                {
                    fixMessage.wrap(buffer, messageOffset, messageHeader.blockLength(), messageHeader.version());

                    return onReplicatedMessage(
                        buffer, offset, length, header, fixMessage.connection());
                }
                case DisconnectDecoder.TEMPLATE_ID:
                {
                    disconnect.wrap(buffer, messageOffset, messageHeader.blockLength(), messageHeader.version());

                    return onReplicatedMessage(
                        buffer, offset, length, header, disconnect.connection());
                }

                default:
                {
                    return handleMessage(buffer, offset, length, header);
                }
            }
        }
        else
        {
            // TODO: generically extract the library id
            final long position = replyPublication.saveNotLeader(0, engineDescriptorStore.leaderLibraryChannel());
        }

        return CONTINUE;
    }

    private Action onReplicatedMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header,
        final long connectionId)
    {
        if (replicatedConnectionIds.contains(connectionId))
        {
            return replicateMessage(buffer, offset, length);
        }
        else
        {
            return handleMessage(buffer, offset, length, header);
        }
    }

    private Action replicateMessage(final DirectBuffer buffer, final int offset, final int length)
    {
        final long position = clusterPublication.tryClaim(length, bufferClaim);
        if (position < 0)
        {
            return ABORT;
        }

        bufferClaim.buffer().putBytes(bufferClaim.offset(), buffer, offset, length);

        bufferClaim.commit();
        return CONTINUE;
    }

    private Action handleMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        DebugLogger.logSbeMessage(buffer, offset);
        return engineProtocolSubscription.onFragment(buffer, offset, length, header);
    }
}
