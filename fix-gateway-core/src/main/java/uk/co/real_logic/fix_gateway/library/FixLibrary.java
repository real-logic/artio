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
package uk.co.real_logic.fix_gateway.library;

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.GatewayProcess;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.messages.ConnectionType;
import uk.co.real_logic.fix_gateway.replication.DataSubscriber;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;
import uk.co.real_logic.fix_gateway.session.*;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;

public class FixLibrary extends GatewayProcess
{
    private final Subscription inboundSubscription;
    private final GatewayPublication outboundPublication;
    private final Long2ObjectHashMap<SessionSubscriber> sessions = new Long2ObjectHashMap<>();
    private final MilliClock clock;
    private final StaticConfiguration configuration;
    private final SessionIdStrategy sessionIdStrategy;

    private Session incomingSession;
    private SessionConfiguration sessionConfiguration;

    public FixLibrary(final StaticConfiguration configuration)
    {
        super(configuration);

        this.configuration = configuration;
        sessionIdStrategy = configuration.sessionIdStrategy();

        inboundSubscription = inboundStreams.dataSubscription();
        outboundPublication = outboundStreams.gatewayPublication();

        clock = System::currentTimeMillis;
    }

    public int poll(final int fragmentLimit)
    {
        return inboundSubscription.poll(dataSubscriber, fragmentLimit) + pollSessions();
    }

    private int pollSessions()
    {
        if (sessions.isEmpty())
        {
            return 0;
        }

        final long time = clock.time();
        int total = 0;
        for (final SessionSubscriber session : sessions.values())
        {
            total += session.poll(time);
        }
        return total;
    }

    public Session initiate(final SessionConfiguration configuration, final IdleStrategy idleStrategy)
    {
        if (sessionConfiguration != null || incomingSession != null)
        {
            throw new IllegalStateException("You can't initiate a session whilst initiating a session");
        }

        sessionConfiguration = configuration;

        outboundPublication.saveInitiateConnection(
            configuration.host(),
            configuration.port(),
            configuration.senderCompId(),
            configuration.targetCompId());

        while (incomingSession == null)
        {
            final int workCount = poll(1);
            idleStrategy.idle(workCount);
        }

        final Session session = incomingSession;
        incomingSession = null;
        sessionConfiguration = null;
        return session;
    }

    private final DataSubscriber dataSubscriber = new DataSubscriber(new SessionHandler()
    {
        public void onConnect(final int libraryId,
                              final long connectionId,
                              final ConnectionType type,
                              final DirectBuffer buffer,
                              final int addressOffset,
                              final int addressLength)
        {
            if (libraryId == outboundPublication.sessionId())
            {
                if (type == INITIATOR)
                {
                    final Session session = initiateSession(connectionId);
                    newSession(connectionId, session);
                    incomingSession = session;
                }
                else
                {
                    final String address = buffer.getStringUtf8(addressOffset, addressLength);
                    final Session session = acceptSession(address, connectionId);
                    newSession(connectionId, session);
                }
            }
            else
            {
                final SessionSubscriber subscriber = sessions.get(connectionId);
                if (subscriber != null)
                {
                    subscriber.onConnect(libraryId, connectionId, type, buffer, addressOffset, addressLength);
                }
            }
        }

        public void onLogon(final long connectionId, final long sessionId)
        {
            final SessionSubscriber subscriber = sessions.get(connectionId);
            if (subscriber != null)
            {
                subscriber.onLogon(connectionId, sessionId);
            }
        }

        public void onMessage(final DirectBuffer buffer,
                              final int offset,
                              final int length,
                              final long connectionId,
                              final long sessionId,
                              final int messageType)
        {
            final SessionSubscriber subscriber = sessions.get(connectionId);
            if (subscriber != null)
            {
                subscriber.onMessage(buffer, offset, length, connectionId, sessionId, messageType);
            }
        }

        public void onDisconnect(final long connectionId)
        {
            final SessionSubscriber subscriber = sessions.get(connectionId);
            if (subscriber != null)
            {
                subscriber.onDisconnect(connectionId);
            }
        }
    });

    private void newSession(final long connectionId, final Session session)
    {
        final SessionParser parser = new SessionParser(
            session, sessionIdStrategy, configuration.authenticationStrategy());
        final SessionHandler handler = configuration.newSessionHandler().onConnect(session);
        final SessionSubscriber subscriber = new SessionSubscriber(parser, session, handler);
        sessions.put(connectionId, subscriber);
    }

    public Session initiateSession(final long connectionId)
    {
        final Object key = sessionIdStrategy.onInitiatorLogon(sessionConfiguration);
        final int defaultInterval = configuration.defaultHeartbeatInterval();
        final GatewayPublication publication = outboundStreams.gatewayPublication();

        return new InitiatorSession(
            defaultInterval,
            connectionId,
            clock,
            sessionProxy(connectionId).setupSession(-1, key),
            publication,
            sessionIdStrategy,
            configuration.beginString(),
            configuration.sendingTimeWindow(),
            fixCounters.receivedMsgSeqNo(connectionId),
            fixCounters.sentMsgSeqNo(connectionId));
    }

    // TODO: refactor to callback
    public Session acceptSession(final String address, final long connectionId)
    {
        final GatewayPublication publication = outboundStreams.gatewayPublication();
        final int defaultInterval = configuration.defaultHeartbeatInterval();

        return new AcceptorSession(
            defaultInterval,
            connectionId,
            clock,
            sessionProxy(connectionId),
            publication,
            sessionIdStrategy,
            configuration.beginString(),
            configuration.sendingTimeWindow(),
            fixCounters.receivedMsgSeqNo(connectionId),
            fixCounters.sentMsgSeqNo(connectionId));
    }

    private SessionProxy sessionProxy(final long connectionId)
    {
        return new SessionProxy(
            configuration.encoderBufferSize(), outboundStreams.gatewayPublication(), sessionIdStrategy,
            configuration.sessionCustomisationStrategy(), System::currentTimeMillis, connectionId);
    }

    // TODO
    private ConnectionTimeoutException timeout(final SessionConfiguration configuration)
    {
        return new ConnectionTimeoutException(
            "Connection timed out connecting to: " + configuration.host() + ":" + configuration.port());
    }

    public void close()
    {
        sessions.values().forEach(SessionSubscriber::close);
        super.close();
    }
}
