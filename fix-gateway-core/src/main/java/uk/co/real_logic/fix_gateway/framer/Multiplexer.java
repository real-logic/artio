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
package uk.co.real_logic.fix_gateway.framer;

import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.commands.ReceiverProxy;
import uk.co.real_logic.fix_gateway.messages.Disconnect;
import uk.co.real_logic.fix_gateway.messages.FixMessage;
import uk.co.real_logic.fix_gateway.messages.MessageHeader;

import static uk.co.real_logic.fix_gateway.replication.GatewayPublication.FRAME_SIZE;

/**
 * Responsible for splitting the data coming out of the replication
 * buffers and pushing it out to the sender end points.
 */
public class Multiplexer implements DataHandler
{

    private final Long2ObjectHashMap<SenderEndPoint> endpoints = new Long2ObjectHashMap<>();

    private final MessageHeader messageHeader = new MessageHeader();
    private final Disconnect disconnect = new Disconnect();
    private final FixMessage messageFrame = new FixMessage();

    private final ReceiverProxy mockReceiver;

    public Multiplexer(final ReceiverProxy mockReceiver)
    {
        this.mockReceiver = mockReceiver;
    }

    public void onNewConnection(final SenderEndPoint senderEndPoint)
    {
        endpoints.put(senderEndPoint.connectionId(), senderEndPoint);
    }

    /**
     * Receive a message from a message subscription buffer.
     */
    public void onMessage(final DirectBuffer buffer, final int offset, final int length, final long connectionId)
    {
        final SenderEndPoint endPoint = endpoints.get(connectionId);
        if (endPoint != null)
        {
            endPoint.onFramedMessage(buffer, offset, length);
        }
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
                final long connectionId = messageFrame.connection();
                onMessage(buffer, offset + FRAME_SIZE, length - (FRAME_SIZE + messageHeader.size()), connectionId);
                break;
            }

            case Disconnect.TEMPLATE_ID:
            {
                disconnect.wrapForDecode(unsafeBuffer, offset, length, 0);
                final long connectionId = disconnect.connection();
                DebugLogger.log("Multiplexer Disconnect: %d\n", connectionId);
                disconnect(connectionId);
                break;
            }
        }
    }

    public void disconnect(final long connectionId)
    {
        endpoints.remove(connectionId);
        mockReceiver.disconnect(connectionId);
    }
}
