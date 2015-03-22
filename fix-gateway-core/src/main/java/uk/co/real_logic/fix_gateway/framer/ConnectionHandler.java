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

import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.FixPublication;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.admin.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.framer.session.*;
import uk.co.real_logic.fix_gateway.replication.ReplicationStreams;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles incoming connections including setting up framers.
 *
 * Threadsafe.
 */
public class ConnectionHandler
{
    private final AtomicLong idSource = new AtomicLong(0);

    private final MilliClock clock;
    private final SessionProxy sessionProxy;
    private final int bufferSize;
    private final int defaultInterval;
    private final SessionIdStrategy sessionIdStrategy;
    private final MessageHandler messageHandler;
    private final ReplicationStreams replicationStreams;
    private final AuthenticationStrategy authenticationStrategy;

    public ConnectionHandler(
        final MilliClock clock,
        final SessionProxy sessionProxy,
        final int bufferSize,
        final int defaultInterval,
        final SessionIdStrategy sessionIdStrategy,
        final MessageHandler messageHandler,
        final ReplicationStreams replicationStreams,
        final AuthenticationStrategy authenticationStrategy)
    {
        this.clock = clock;
        this.sessionProxy = sessionProxy;
        this.bufferSize = bufferSize;
        this.defaultInterval = defaultInterval;
        this.sessionIdStrategy = sessionIdStrategy;
        this.messageHandler = messageHandler;
        this.replicationStreams = replicationStreams;
        this.authenticationStrategy = authenticationStrategy;
    }

    public long onConnection() throws IOException
    {
        return idSource.getAndIncrement();
    }

    public ReceiverEndPoint receiverEndPoint(
        final SocketChannel channel, final long connectionId, final Session session)
    {
        final SessionParser sessionParser = new SessionParser(session, sessionIdStrategy, authenticationStrategy);
        return new ReceiverEndPoint(channel, bufferSize, messageHandler, connectionId, sessionParser);
    }

    public SenderEndPoint senderEndPoint(final SocketChannel channel, final long connectionId)
    {
        return new SenderEndPoint(connectionId, channel);
    }

    public AcceptorSession acceptSession(final long connectionId)
    {
        return new AcceptorSession(defaultInterval, connectionId, clock, sessionProxy);
    }

    public InitiatorSession initiateSession(
        final long connectionId, final FixGateway gateway, final SessionConfiguration configuration)
    {
        final long sessionId = sessionIdStrategy.register(configuration);
        final FixPublication fixPublication = replicationStreams.fixPublication();

        return new InitiatorSession(defaultInterval, connectionId, clock, sessionProxy, gateway, fixPublication,
            sessionId, sessionIdStrategy);
    }
}
