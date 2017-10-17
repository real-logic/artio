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
package uk.co.real_logic.fix_gateway.engine.framer;

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ExclusiveBufferClaim;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongHashSet;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.EngineDescriptorStore;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.protocol.EngineProtocolSubscription;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.replication.ClusterablePublication;
import uk.co.real_logic.fix_gateway.replication.ClusterableStreams;
import uk.co.real_logic.fix_gateway.sbe_util.IdExtractor;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.LogTag.GATEWAY_MESSAGE;

/**
 * Splits the subscription out into messages that we deal with locally vs cluster
 */
class SubscriptionSplitter implements ControlledFragmentHandler
{
    private static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;

    private final ExclusiveBufferClaim bufferClaim = new ExclusiveBufferClaim();

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final DisconnectDecoder disconnect = new DisconnectDecoder();
    private final IdExtractor idExtractor = new IdExtractor();

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ReplicatedMessageEncoder replicatedMessage = new ReplicatedMessageEncoder();

    private final ClusterableStreams clusterableStreams;
    private final EngineProtocolSubscription engineProtocolSubscription;
    private final ClusterablePublication clusterPublication;
    private final GatewayPublication replyToLibraryPublication;
    private final EngineDescriptorStore engineDescriptorStore;
    private final String bindAddress;
    private final LongHashSet replicatedConnectionIds;

    SubscriptionSplitter(
        final ClusterableStreams clusterableStreams,
        final EngineProtocolSubscription engineProtocolSubscription,
        final ClusterablePublication clusterPublication,
        final GatewayPublication replyToLibraryPublication,
        final EngineDescriptorStore engineDescriptorStore,
        final String bindAddress,
        final LongHashSet replicatedConnectionIds)
    {
        this.clusterableStreams = clusterableStreams;
        this.engineProtocolSubscription = engineProtocolSubscription;
        this.clusterPublication = clusterPublication;
        this.replyToLibraryPublication = replyToLibraryPublication;
        this.engineDescriptorStore = engineDescriptorStore;
        this.bindAddress = bindAddress;
        this.replicatedConnectionIds = replicatedConnectionIds;
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final MessageHeaderDecoder messageHeaderDecoder = this.messageHeaderDecoder;

        messageHeaderDecoder.wrap(buffer, offset);

        final int messageOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int actingBlockLength = messageHeaderDecoder.blockLength();
        final int version = messageHeaderDecoder.version();
        final int templateId = messageHeaderDecoder.templateId();

        if (clusterableStreams.isLeader())
        {
            switch (templateId)
            {
                case FixMessageDecoder.TEMPLATE_ID:
                {
                    final FixMessageDecoder fixMessage = this.fixMessage;
                    fixMessage.wrap(buffer, messageOffset, actingBlockLength, version);

                    final long connection = fixMessage.connection();
                    final int libraryId = fixMessage.libraryId();

                    return onReplicatedMessage(buffer, offset, length, header, connection, libraryId);
                }

                case DisconnectDecoder.TEMPLATE_ID:
                {
                    final DisconnectDecoder disconnect = this.disconnect;
                    disconnect.wrap(buffer, messageOffset, actingBlockLength, version);

                    final long connection = disconnect.connection();
                    final int libraryId = disconnect.libraryId();

                    return onReplicatedMessage(buffer, offset, length, header, connection, libraryId);
                }

                default:
                {
                    return handleMessage(buffer, offset, length, header);
                }
            }
        }
        else
        {
            idExtractor.decode(
                buffer,
                messageOffset,
                actingBlockLength,
                version,
                templateId);

            final int libraryId = idExtractor.libraryId();
            final long correlationId = idExtractor.correlationId();

            final long position = replyToLibraryPublication.saveNotLeader(
                libraryId,
                correlationId,
                engineDescriptorStore.leaderLibraryChannel());

            //System.out.println("NOT LEADER position = " + position);
            //System.out.println("NOT LEADER libraryId = " + libraryId);
            //System.out.println("NOT LEADER correlationId = " + correlationId);
        }

        return CONTINUE;
    }

    private Action onReplicatedMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header,
        final long connectionId,
        final int libraryId)
    {
        if (replicatedConnectionIds.contains(connectionId))
        {
            return replicateMessage(buffer, offset, length, header, libraryId);
        }
        else
        {
            return handleMessage(buffer, offset, length, header);
        }
    }

    private Action replicateMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header,
        final int libraryId)
    {
        final int requiredLength = HEADER_LENGTH + ReplicatedMessageEncoder.BLOCK_LENGTH + length;
        final long position = clusterPublication.tryClaim(requiredLength, bufferClaim);
        if (position < 0)
        {
            return ABORT;
        }

        final MutableDirectBuffer clusterBuffer = bufferClaim.buffer();
        int clusterOffset = bufferClaim.offset();

        final ReplicatedMessageEncoder replicatedMessage = this.replicatedMessage;

        replicatedMessage
            .wrapAndApplyHeader(clusterBuffer, clusterOffset, messageHeaderEncoder)
            .libraryId(libraryId)
            .position(header.position());

        clusterOffset += MessageHeaderEncoder.ENCODED_LENGTH + ReplicatedMessageEncoder.BLOCK_LENGTH;

        clusterBuffer.putBytes(clusterOffset, buffer, offset, length);

        bufferClaim.commit();
        return CONTINUE;
    }

    private Action handleMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        // TODO: identify generic toString() approach in SBE
        DebugLogger.logSbeMessage(GATEWAY_MESSAGE, buffer, offset);
        return engineProtocolSubscription.onFragment(buffer, offset, length, header);
    }
}
