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

import uk.co.real_logic.fix_gateway.framer.ReceiverEndPoint;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;
import uk.co.real_logic.fix_gateway.replication.ReplicationStreams;
import uk.co.real_logic.fix_gateway.framer.SenderEndPoint;
import uk.co.real_logic.fix_gateway.session.*;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles incoming connections including setting up framers.
 *
 * Threadsafe.
 */
// TODO: refactor out this class - its ended up absorbing connection and session concerns
public class ConnectionHandler
{
    private final AtomicLong idSource = new AtomicLong(0);

    private final MilliClock clock;
    private final StaticConfiguration configuration;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionIds sessionIds;
    private final ReplicationStreams inboundStreams;
    private final ReplicationStreams outboundStreams;

    public ConnectionHandler(
        final MilliClock clock,
        final StaticConfiguration configuration,
        final SessionIdStrategy sessionIdStrategy,
        final SessionIds sessionIds,
        final ReplicationStreams inboundStreams,
        final ReplicationStreams outboundStreams)
    {
        this.clock = clock;
        this.configuration = configuration;
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionIds = sessionIds;
        this.inboundStreams = inboundStreams;
        this.outboundStreams = outboundStreams;
    }

    public long onConnection() throws IOException
    {
        return idSource.getAndIncrement();
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
        return new SenderEndPoint(connectionId, channel);
    }

    public AcceptorSession acceptSession(final long connectionId)
    {
        final GatewayPublication publication = outboundStreams.gatewayPublication();
        final int defaultInterval = configuration.defaultHeartbeatInterval();
        return new AcceptorSession(
            defaultInterval, connectionId, clock, sessionProxy(), publication, sessionIdStrategy);
    }

    public InitiatorSession initiateSession(
        final long connectionId, final FixGateway gateway, final SessionConfiguration sessionConfiguration)
    {
        final Object key = sessionIdStrategy.onInitiatorLogon(sessionConfiguration);
        final long sessionId = sessionIds.onLogon(key);
        final int defaultInterval = configuration.defaultHeartbeatInterval();
        final GatewayPublication gatewayPublication = outboundStreams.gatewayPublication();

        return new InitiatorSession(
            defaultInterval,
            connectionId,
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
}
