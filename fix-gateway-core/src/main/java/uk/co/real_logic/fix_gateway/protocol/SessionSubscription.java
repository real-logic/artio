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
import uk.co.real_logic.fix_gateway.library.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.DisconnectDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.OK;
import static uk.co.real_logic.fix_gateway.protocol.GatewayPublication.FRAME_SIZE;

public final class SessionSubscription implements ControlledFragmentHandler
{

    private static final Action UNKNOWN_TEMPLATE = null;

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final DisconnectDecoder disconnect = new DisconnectDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();

    private final SessionHandler sessionHandler;
    private final Action defaultAction;

    public static SessionSubscription of(final SessionHandler sessionHandler)
    {
        return new SessionSubscription(sessionHandler, CONTINUE);
    }

    public static ControlledFragmentHandler of(final SessionHandler sessionHandler, final ControlledFragmentHandler other)
    {
        final SessionSubscription subscription = new SessionSubscription(sessionHandler, UNKNOWN_TEMPLATE);
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

    private SessionSubscription(final SessionHandler sessionHandler,
                                final Action defaultAction)
    {
        this.sessionHandler = sessionHandler;
        this.defaultAction = defaultAction;
    }

    public Action onFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        messageHeader.wrap(buffer, offset);

        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();
        offset += messageHeader.encodedLength();

        switch (messageHeader.templateId())
        {
            case FixMessageDecoder.TEMPLATE_ID:
            {
                return onFixMessage(buffer, offset, blockLength, version, header);
            }

            case DisconnectDecoder.TEMPLATE_ID:
            {
                return onDisconnect(buffer, offset, blockLength, version);
            }
        }

        return defaultAction;
    }

    private Action onDisconnect(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version)
    {
        disconnect.wrap(buffer, offset, blockLength, version);
        final long connectionId = disconnect.connection();
        DebugLogger.log("FixSubscription Disconnect: %d\n", connectionId);
        return sessionHandler.onDisconnect(disconnect.libraryId(), connectionId, disconnect.reason());
    }

    private Action onFixMessage(
        final DirectBuffer buffer, final int offset, final int blockLength, final int version, final Header header)
    {
        messageFrame.wrap(buffer, offset, blockLength, version);
        final int messageLength = messageFrame.bodyLength();
        if (messageFrame.status() == OK)
        {
            return sessionHandler.onMessage(
                buffer,
                offset + FRAME_SIZE,
                messageLength,
                messageFrame.libraryId(),
                messageFrame.connection(),
                messageFrame.session(),
                messageFrame.messageType(),
                messageFrame.timestamp(),
                header.position());
        }

        return CONTINUE;
    }
}
