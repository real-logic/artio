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
package uk.co.real_logic.fix_gateway.engine.framer;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

public class SenderEndPoints implements AutoCloseable
{
    private final Long2ObjectHashMap<SenderEndPoint> connectionIdToSenderEndpoint = new Long2ObjectHashMap<>();

    public void add(final SenderEndPoint senderEndPoint)
    {
        connectionIdToSenderEndpoint.put(senderEndPoint.connectionId(), senderEndPoint);
    }

    public void removeConnection(final long connectionId)
    {
        final SenderEndPoint senderEndPoint = connectionIdToSenderEndpoint.remove(connectionId);
        if (senderEndPoint != null)
        {
            senderEndPoint.close();
        }
    }

    public void removeLibrary(final int libraryId)
    {
        connectionIdToSenderEndpoint
            .values()
            .removeIf(senderEndPoint ->
            {
                final boolean remove = senderEndPoint.libraryId() == libraryId;
                if (remove)
                {
                    senderEndPoint.close();
                }
                return remove;
            });
    }

    public void onMessage(
        final long connectionId, final DirectBuffer buffer, final int offset, final int length)
    {
        final SenderEndPoint endPoint = connectionIdToSenderEndpoint.get(connectionId);
        if (endPoint != null)
        {
            endPoint.onFramedMessage(buffer, offset, length);
        }
    }

    public void close()
    {
        connectionIdToSenderEndpoint
            .values()
            .forEach(SenderEndPoint::close);
    }
}
