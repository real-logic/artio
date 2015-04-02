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
package uk.co.real_logic.fix_gateway.replication;

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.admin.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.Disconnect;
import uk.co.real_logic.fix_gateway.messages.FixMessage;
import uk.co.real_logic.fix_gateway.messages.MessageHeader;

import static uk.co.real_logic.fix_gateway.replication.GatewayPublication.FRAME_SIZE;

public class GatewaySubscription
{

    private final MessageHeader messageHeader = new MessageHeader();
    private final Disconnect disconnect = new Disconnect();
    private final FixMessage messageFrame = new FixMessage();
    private final Subscription subscription;

    private SessionHandler sessionHandler;

    public GatewaySubscription(final ReplicationStreams replicationStreams)
    {
        subscription = replicationStreams.dataSubscription(this::onData);
    }

    public GatewaySubscription sessionHandler(final SessionHandler sessionHandler)
    {
        this.sessionHandler = sessionHandler;
        return this;
    }

    public void onData(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        // TODO:
        final UnsafeBuffer unsafeBuffer = (UnsafeBuffer) buffer;
        messageHeader.wrap(unsafeBuffer, offset, 0);

        offset += messageHeader.size();

        switch (messageHeader.templateId())
        {
            case FixMessage.TEMPLATE_ID:
            {
                messageFrame.wrapForDecode(unsafeBuffer, offset, length, 0);
                final int messageLength = length - (FRAME_SIZE + messageHeader.size());
                sessionHandler.onMessage(
                    buffer,
                    offset + FRAME_SIZE,
                    messageLength,
                    messageFrame.connection(),
                    messageFrame.session(),
                    messageFrame.messageType());
                break;
            }

            case Disconnect.TEMPLATE_ID:
            {
                disconnect.wrapForDecode(unsafeBuffer, offset, length, 0);
                final long connectionId = disconnect.connection();
                DebugLogger.log("FixSubscription Disconnect: %d\n", connectionId);
                sessionHandler.onDisconnect(connectionId);
                break;
            }
        }
    }

    public int poll(final int limit)
    {
        return subscription.poll(limit);
    }
}
