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

import uk.co.real_logic.agrona.collections.LongHashSet;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.framer.ReceiverEndPoint;
import uk.co.real_logic.fix_gateway.framer.SenderEndPoint;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;
import uk.co.real_logic.fix_gateway.replication.ReplicationStreams;
import uk.co.real_logic.fix_gateway.session.*;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import java.io.IOException;
import java.net.SocketAddress;
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
    private final LongHashSet acceptedSessions = new LongHashSet(40, -1);

    private final MilliClock clock;
    private final StaticConfiguration configuration;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionIds sessionIds;
    private final ReplicationStreams inboundStreams;
    private final ReplicationStreams outboundStreams;
    private final IdleStrategy idleStrategy;
    private final FixCounters fixCounters;

    public ConnectionHandler(
        final MilliClock clock,
        final StaticConfiguration configuration,
        final SessionIdStrategy sessionIdStrategy,
        final SessionIds sessionIds,
        final ReplicationStreams inboundStreams,
        final ReplicationStreams outboundStreams,
        final IdleStrategy idleStrategy,
        final FixCounters fixCounters)
    {
        this.clock = clock;
        this.configuration = configuration;
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionIds = sessionIds;
        this.inboundStreams = inboundStreams;
        this.outboundStreams = outboundStreams;
        this.idleStrategy = idleStrategy;
        this.fixCounters = fixCounters;
    }

    public ReceiverEndPoint receiverEndPoint(
        final SocketChannel channel, final long connectionId, final Session session) throws IOException
    {
        final SessionParser sessionParser = new SessionParser(session, sessionIdStrategy, sessionIds,
            configuration.authenticationStrategy());

        configuration.newSessionHandler().onConnect(session, inboundStreams.dataSubscription());

        return new ReceiverEndPoint(
            channel,
            configuration.receiverBufferSize(),
            inboundStreams.gatewayPublication(),
            connectionId,
            sessionParser,
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

    public Session acceptSession(final SocketAddress address)
    {
        final GatewayPublication publication = outboundStreams.gatewayPublication();
        final int defaultInterval = configuration.defaultHeartbeatInterval();
        final long connectionId = nextConnectionId();

        publication.saveConnect(connectionId, address);

        return new AcceptorSession(
            defaultInterval,
            connectionId,
            clock,
            sessionProxy(),
            publication,
            sessionIdStrategy,
            configuration.beginString(),
            configuration.sendingTimeWindow(),
            sessionIds,
            acceptedSessions,
            fixCounters.receivedMsgSeqNo(connectionId),
            fixCounters.sentMsgSeqNo(connectionId));
    }

    public Session initiateSession(final SocketAddress address,
                                   final FixGateway gateway,
                                   final SessionConfiguration sessionConfiguration)
    {
        final Object key = sessionIdStrategy.onInitiatorLogon(sessionConfiguration);
        final long sessionId = sessionIds.onLogon(key);
        final int defaultInterval = configuration.defaultHeartbeatInterval();
        final GatewayPublication publication = outboundStreams.gatewayPublication();
        final long connectionId = nextConnectionId();

        publication.saveConnect(connectionId, address);

        return new InitiatorSession(
            defaultInterval,
            connectionId,
            clock,
            sessionProxy().setupSession(sessionId, key),
            publication,
            sessionIdStrategy,
            gateway,
            sessionId,
            configuration.beginString(),
            configuration.sendingTimeWindow(),
            sessionIds,
            fixCounters.receivedMsgSeqNo(connectionId),
            fixCounters.sentMsgSeqNo(connectionId));
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
