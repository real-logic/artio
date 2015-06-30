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
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.auth.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.replication.DataSubscriber;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;
import uk.co.real_logic.fix_gateway.session.*;

public class FixLibrary extends GatewayProcess
{
    private final Subscription inboundSubscription;
    private final GatewayPublication outboundPublication;
    private final Long2ObjectHashMap<SessionSubscriber> sessions = new Long2ObjectHashMap<>();
    private final NewSessionHandler newSessionHandler;
    private final SessionIdStrategy sessionIdStrategy;
    private final AuthenticationStrategy authenticationStrategy;

    private Session incomingSession;

    public FixLibrary(final StaticConfiguration configuration)
    {
        super(configuration);

        newSessionHandler = configuration.newSessionHandler();
        sessionIdStrategy = configuration.sessionIdStrategy();
        authenticationStrategy = configuration.authenticationStrategy();

        inboundSubscription = inboundStreams.dataSubscription();
        outboundPublication = outboundStreams.gatewayPublication();
    }

    public int poll(final int fragmentLimit)
    {
        return inboundSubscription.poll(dataSubscriber, fragmentLimit);
    }

    public Session initiate(final SessionConfiguration configuration, final IdleStrategy idleStrategy)
    {
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
        return session;
    }

    private final DataSubscriber dataSubscriber = new DataSubscriber(new SessionHandler()
    {
        public void onConnect(final int streamId,
                              final long connectionId,
                              final DirectBuffer buffer,
                              final int addressOffset,
                              final int addressLength)
        {
            if (streamId == outboundPublication.streamId())
            {
                final InitiatorSession session = null; //new InitiatorSession();
                final SessionParser parser = new SessionParser(session, sessionIdStrategy, authenticationStrategy);
                final SessionHandler handler = newSessionHandler.onConnect(session);
                final SessionSubscriber subscriber = new SessionSubscriber(parser, session, handler);
                sessions.put(connectionId, subscriber);
            }
            else
            {
                final SessionSubscriber subscriber = sessions.get(connectionId);
                if (subscriber != null)
                {
                    subscriber.onConnect(streamId, connectionId, buffer, addressOffset, addressLength);
                }
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

}
