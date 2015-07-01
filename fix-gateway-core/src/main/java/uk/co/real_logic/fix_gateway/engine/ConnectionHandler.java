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
package uk.co.real_logic.fix_gateway.engine;

import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.engine.framer.ReceiverEndPoint;
import uk.co.real_logic.fix_gateway.engine.framer.SenderEndPoint;
import uk.co.real_logic.fix_gateway.replication.ReplicationStreams;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.session.SessionIds;

import java.io.IOException;
import java.nio.channels.SocketChannel;

/**
 * Handles incoming connections including setting up framers.
 *
 * Threadsafe.
 */
public class ConnectionHandler
{
    private final StaticConfiguration configuration;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionIds sessionIds;
    private final ReplicationStreams inboundStreams;
    private final IdleStrategy idleStrategy;
    private final FixCounters fixCounters;

    public ConnectionHandler(
        final StaticConfiguration configuration,
        final SessionIdStrategy sessionIdStrategy,
        final SessionIds sessionIds,
        final ReplicationStreams inboundStreams,
        final IdleStrategy idleStrategy,
        final FixCounters fixCounters)
    {
        this.configuration = configuration;
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionIds = sessionIds;
        this.inboundStreams = inboundStreams;
        this.idleStrategy = idleStrategy;
        this.fixCounters = fixCounters;
    }

    public ReceiverEndPoint receiverEndPoint(
        final SocketChannel channel, final long connectionId, final long sessionId) throws IOException
    {
        return new ReceiverEndPoint(
            channel,
            configuration.receiverBufferSize(),
            inboundStreams.gatewayPublication(),
            connectionId,
            sessionId,
            sessionIdStrategy,
            sessionIds,
            fixCounters.messagesRead(channel.getRemoteAddress())
        );
    }

    public SenderEndPoint senderEndPoint(final SocketChannel channel, final long connectionId) throws IOException
    {
        return new SenderEndPoint(
            connectionId,
            channel,
            idleStrategy,
            fixCounters.messagesWritten(channel.getRemoteAddress()));
    }
}
