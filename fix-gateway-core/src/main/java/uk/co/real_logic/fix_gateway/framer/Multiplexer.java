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

/**
 * Responsible for splitting the data coming out of the replication
 * buffers and pushing it out to the sender end points.
 */
public class Multiplexer implements DataHandler
{
    private final Long2ObjectHashMap<SenderEndPoint> endpoints = new Long2ObjectHashMap<>();

    public void onNewConnection(final SenderEndPoint senderEndPoint)
    {
        endpoints.put(senderEndPoint.connectionId(), senderEndPoint);
    }

    /**
     * Receive a message from a message subscription buffer.
     */
    public void onMessage(final DirectBuffer buffer, final int offset, final int length, final long sessionId)
    {
        final SenderEndPoint endPoint = endpoints.get(sessionId);
        if (endPoint != null)
        {
            endPoint.onFramedMessage(buffer, offset, length);
        }
    }

    public void onData(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        // TODO: read framed fix message out of buffer
    }
}
