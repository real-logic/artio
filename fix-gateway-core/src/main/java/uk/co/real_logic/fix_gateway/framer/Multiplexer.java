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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.fix_gateway.admin.SessionHandler;
import uk.co.real_logic.fix_gateway.commands.ReceiverProxy;

/**
 * Responsible for splitting the data coming out of the replication
 * buffers and pushing it out to the sender end points.
 */
public class Multiplexer implements SessionHandler
{

    private final Long2ObjectHashMap<SenderEndPoint> endpoints = new Long2ObjectHashMap<>();

    private final ReceiverProxy receiver;

    public Multiplexer(final ReceiverProxy receiver)
    {
        this.receiver = receiver;
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

    public void onDisconnect(final long connectionId)
    {
        endpoints.remove(connectionId);
        receiver.disconnect(connectionId);
    }

}
