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

import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

/**
 * Responsible for splitting the data coming out of the replication
 * buffers and pushing it out to the sender end points.
 */
public class Multiplexer
{
    private final Long2ObjectHashMap<Connection> connections = new Long2ObjectHashMap<>();

    public void onNewConnection(final Connection connection)
    {
        connections.put(connection.connectionId(), connection);
    }

    public int scanBuffers()
    {
        // TODO: read message off buffers
        // TODO: lookup connection id from the message
        // TODO: mux message to correct sender end point by connection id
        return 0;
    }
}
