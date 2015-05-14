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
package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.framer.ReceiverEndPoint;
import uk.co.real_logic.fix_gateway.framer.SenderEndPoint;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;
import uk.co.real_logic.fix_gateway.replication.ReplicationStreams;
import uk.co.real_logic.fix_gateway.session.*;
import uk.co.real_logic.fix_gateway.util.MilliClock;

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
    private final StaticConfiguration configuration;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionIds sessionIds;
    private final ReplicationStreams inboundStreams;
    private final ReplicationStreams outboundStreams;
    private final IdleStrategy idleStrategy;

    public ConnectionHandler(
        final MilliClock clock,
        final StaticConfiguration configuration,
        final SessionIdStrategy sessionIdStrategy,
        final SessionIds sessionIds,
        final ReplicationStreams inboundStreams,
        final ReplicationStreams outboundStreams,
        final IdleStrategy idleStrategy)
    {
        this.clock = clock;
        this.configuration = configuration;
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionIds = sessionIds;
        this.inboundStreams = inboundStreams;
        this.outboundStreams = outboundStreams;
        this.idleStrategy = idleStrategy;
    }

    public ReceiverEndPoint receiverEndPoint(
        final SocketChannel channel, final long connectionId, final Session session)
    {
        final SessionParser sessionParser = new SessionParser(session, sessionIdStrategy, sessionIds,
            configuration.authenticationStrategy());

        configuration.newSessionHandler().onConnect(session, inboundStreams.gatewaySubscription());

        return new ReceiverEndPoint(
            channel,
            configuration.receiverBufferSize(),
            inboundStreams.gatewayPublication(),
            connectionId,
            sessionParser
        );
    }

    public SenderEndPoint senderEndPoint(final SocketChannel channel, final long connectionId)
    {
        return new SenderEndPoint(connectionId, channel, idleStrategy);
    }

    public Session acceptSession()
    {
        final GatewayPublication publication = outboundStreams.gatewayPublication();
        final int defaultInterval = configuration.defaultHeartbeatInterval();
        return new AcceptorSession(
            defaultInterval, nextConnectionId(), clock, sessionProxy(), publication, sessionIdStrategy);
    }

    public Session initiateSession(final FixGateway gateway, final SessionConfiguration sessionConfiguration)
    {
        final Object key = sessionIdStrategy.onInitiatorLogon(sessionConfiguration);
        final long sessionId = sessionIds.onLogon(key);
        final int defaultInterval = configuration.defaultHeartbeatInterval();
        final GatewayPublication gatewayPublication = outboundStreams.gatewayPublication();

        return new InitiatorSession(
            defaultInterval,
            nextConnectionId(),
            clock,
            sessionProxy().setupSession(sessionId, key),
            gatewayPublication,
            sessionIdStrategy,
            gateway,
            sessionId);
    }

    private SessionProxy sessionProxy()
    {
        return new SessionProxy(
            configuration.encoderBufferSize(), outboundStreams.gatewayPublication(), sessionIdStrategy);
    }

    private long nextConnectionId()
    {
        return idSource.getAndIncrement();
    }
}
