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
package uk.co.real_logic.fix_gateway.protocol;

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.messages.DisconnectDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.ReplicatedMessageDecoder;
import uk.co.real_logic.fix_gateway.replication.ClusterFragmentHandler;
import uk.co.real_logic.fix_gateway.replication.ClusterHeader;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.LogTag.FIX_MESSAGE;
import static uk.co.real_logic.fix_gateway.protocol.GatewayPublication.FRAME_SIZE;

public final class ProtocolSubscription implements ControlledFragmentHandler, ClusterFragmentHandler
{
    private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;

    private static final Action UNKNOWN_TEMPLATE = null;

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final DisconnectDecoder disconnect = new DisconnectDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();
    private final ReplicatedMessageDecoder replicatedMessage = new ReplicatedMessageDecoder();

    private final ProtocolHandler protocolHandler;
    private final Action defaultAction;

    public static ProtocolSubscription of(final ProtocolHandler protocolHandler)
    {
        return new ProtocolSubscription(protocolHandler, CONTINUE);
    }

    public static ControlledFragmentHandler of(
        final ProtocolHandler protocolHandler, final ControlledFragmentHandler other)
    {
        final ProtocolSubscription subscription = new ProtocolSubscription(protocolHandler, UNKNOWN_TEMPLATE);
        return (buffer, offset, length, header) ->
        {
            final Action action = subscription.onFragment(buffer, offset, length, header);

            if (action == UNKNOWN_TEMPLATE)
            {
                return other.onFragment(buffer, offset, length, header);
            }

            return action;
        };
    }

    private ProtocolSubscription(final ProtocolHandler protocolHandler, final Action defaultAction)
    {
        this.protocolHandler = protocolHandler;
        this.defaultAction = defaultAction;
    }

    public Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final ClusterHeader header)
    {
        return onFragment(buffer, offset, length, header.position());
    }

    public Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        return onFragment(buffer, offset, length, header.position());
    }

    @SuppressWarnings("FinalParameters")
    private Action onFragment(final DirectBuffer buffer, int offset, int length, final long position)
    {
        messageHeader.wrap(buffer, offset);

        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();
        offset += MessageHeaderDecoder.ENCODED_LENGTH;

        switch (messageHeader.templateId())
        {
            case FixMessageDecoder.TEMPLATE_ID:
            {
                return onFixMessage(buffer, offset, blockLength, version, position);
            }

            case DisconnectDecoder.TEMPLATE_ID:
            {
                return onDisconnect(buffer, offset, blockLength, version);
            }

            case ReplicatedMessageDecoder.TEMPLATE_ID:
            {
                // Skip over replicated message header to its payload
                offset += ReplicatedMessageDecoder.BLOCK_LENGTH;
                length -= HEADER_LENGTH + ReplicatedMessageDecoder.BLOCK_LENGTH;
                return onFragment(buffer, offset, length, position);
            }
        }

        return defaultAction;
    }

    private Action onDisconnect(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        disconnect.wrap(buffer, offset, blockLength, version);
        final long connectionId = disconnect.connection();
        DebugLogger.log(FIX_MESSAGE, "FixSubscription Disconnect: %d%n", connectionId);
        return protocolHandler.onDisconnect(disconnect.libraryId(), connectionId, disconnect.reason());
    }

    private Action onFixMessage(
        final DirectBuffer buffer,
        final int offset,
        final int blockLength,
        final int version,
        final long position)
    {
        messageFrame.wrap(buffer, offset, blockLength, version);
        final int messageLength = messageFrame.bodyLength();
        return protocolHandler.onMessage(
            buffer,
            offset + FRAME_SIZE,
            messageLength,
            messageFrame.libraryId(),
            messageFrame.connection(),
            messageFrame.session(),
            messageFrame.sequenceIndex(),
            messageFrame.messageType(),
            messageFrame.timestamp(),
            messageFrame.status(),
            position);
    }
}
