/*
 * Copyright 2020 Monotonic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.engine.framer;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

public class ILink3SenderEndPoints
{
    private final Long2ObjectHashMap<ILink3SenderEndPoint> connectionIdToSenderEndpoint = new Long2ObjectHashMap<>();

    public Action onMessage(final long connectionId, final DirectBuffer buffer, final int offset)
    {
        final ILink3SenderEndPoint senderEndPoint = connectionIdToSenderEndpoint.get(connectionId);
        if (senderEndPoint != null)
        {
            return senderEndPoint.onMessage(buffer, offset);
        }
        return CONTINUE;
    }

    public void add(final ILink3SenderEndPoint senderEndPoint)
    {
        connectionIdToSenderEndpoint.put(senderEndPoint.connectionId(), senderEndPoint);
    }

    void removeConnection(final long connectionId)
    {
        connectionIdToSenderEndpoint.remove(connectionId);
    }

    public Action onReplayComplete(final long connectionId)
    {
        final ILink3SenderEndPoint senderEndPoint = connectionIdToSenderEndpoint.get(connectionId);
        if (senderEndPoint != null)
        {
            return senderEndPoint.onReplayComplete(connectionId);
        }
        return CONTINUE;
    }

    public String toString()
    {
        return "ILink3SenderEndPoints{" +
            "connectionIdToSenderEndpoint=" + connectionIdToSenderEndpoint +
            '}';
    }
}
