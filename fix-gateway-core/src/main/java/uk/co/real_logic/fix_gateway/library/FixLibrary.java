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
import uk.co.real_logic.agrona.concurrent.*;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.FixGatewayException;
import uk.co.real_logic.fix_gateway.GatewayProcess;
import uk.co.real_logic.fix_gateway.library.session.*;
import uk.co.real_logic.fix_gateway.library.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.MessageValidationStrategy;
import uk.co.real_logic.fix_gateway.messages.ConnectionType;
import uk.co.real_logic.fix_gateway.messages.GatewayError;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.streams.ActivationHandler;
import uk.co.real_logic.fix_gateway.streams.DataSubscriber;
import uk.co.real_logic.fix_gateway.streams.GatewayPublication;

import java.util.List;
import java.util.function.Consumer;

import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.fix_gateway.messages.GatewayError.UNABLE_TO_CONNECT;

public class FixLibrary extends GatewayProcess
{
    private QueuedPipe<LibraryCommand> commands = new OneToOneConcurrentArrayQueue<>(16);

    private final Consumer<LibraryCommand> executeFunc = command -> command.execute(this);
    private final Subscription inboundSubscription;
    private final GatewayPublication outboundPublication;
    private final Long2ObjectHashMap<SessionSubscriber> sessions = new Long2ObjectHashMap<>();
    private final EpochClock clock;
    private final LibraryConfiguration configuration;
    private final SessionIdStrategy sessionIdStrategy;

    private Session incomingSession;
    private SessionConfiguration sessionConfiguration;

    private GatewayError errorType;
    private String errorMessage;

    private boolean connected = false;

    public FixLibrary(final LibraryConfiguration configuration)
    {
        configuration.activationHandler(
            new ActivationHandler(commands, configuration.aeronChannel(), INBOUND_LIBRARY_STREAM));

        init(configuration);

        this.configuration = configuration;
        sessionIdStrategy = configuration.sessionIdStrategy();

        inboundSubscription = inboundLibraryStreams.subscription();
        outboundPublication = outboundLibraryStreams.gatewayPublication();

        clock = new SystemEpochClock();
    }

    public int poll(final int fragmentLimit)
    {
        return commands.drain(executeFunc) +
               inboundSubscription.poll(dataSubscriber, fragmentLimit) +
               pollSessions();
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
        if (sessionConfiguration != null || incomingSession != null || errorType != null)
        {
            throw new IllegalStateException("You can't initiate a session whilst initiating a session");
        }

        sessionConfiguration = configuration;

        try
        {
            final long replyTimeoutInMs = this.configuration.replyTimeoutInMs();
            final List<String> hosts = configuration.hosts();
            final List<Integer> ports = configuration.ports();
            final int size = hosts.size();
            for (int i = 0; i < size; i++)
            {
                final String host = hosts.get(i);
                final int port = ports.get(i);

                outboundPublication.saveInitiateConnection(
                    host,
                    port,
                    configuration.senderCompId(),
                    configuration.senderSubId(),
                    configuration.senderLocationId(),
                    configuration.targetCompId());

                final long latestReplyArrivalTime = clock.time() + replyTimeoutInMs;
                while (incomingSession == null && errorType == null)
                {
                    final int workCount = poll(1);

                    if (clock.time() > latestReplyArrivalTime)
                    {
                        throw new IllegalStateException(String.format(
                            "Failed to receive a reply from the engine within %dms, are you sure its running?",
                            replyTimeoutInMs));
                    }

                    idleStrategy.idle(workCount);
                }

                if (incomingSession != null)
                {
                    final Session session = incomingSession;
                    session.address(host, port);
                    return session;
                }
                else if (errorType != UNABLE_TO_CONNECT)
                {
                    throw new FixGatewayException(String.format("%s: %s", errorType, errorMessage));
                }

                errorType = null;
            }

            throw new FixGatewayException("Unable to connect to any of the addresses specified");
        }
        finally
        {
            sessionConfiguration = null;
            errorType = null;
            incomingSession = null;
        }
    }

    private final DataSubscriber dataSubscriber = new DataSubscriber(new SessionHandler()
    {

        public void onConnect(
            final int libraryId,
            final long connectionId,
            final ConnectionType type,
            final DirectBuffer buffer,
            final int addressOffset,
            final int addressLength)
        {
            if (type == INITIATOR)
            {
                DebugLogger.log("Init Connect: %d, %d\n", connectionId, libraryId);
                final Session session = initiateSession(connectionId);
                newSession(connectionId, session);
                incomingSession = session;
            }
            else
            {
                DebugLogger.log("Acct Connect: %d, %d\n", connectionId, libraryId);
                final Session session = acceptSession(connectionId);
                newSession(connectionId, session);
            }
        }

        public void onLogon(final long connectionId, final long sessionId)
        {
            DebugLogger.log("Library Logon: %d, %d\n", connectionId, sessionId);
            final SessionSubscriber subscriber = sessions.get(connectionId);
            if (subscriber != null)
            {
                subscriber.onLogon(connectionId, sessionId);
            }
        }

        public void onMessage(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final long connectionId,
            final long sessionId,
            final int messageType)
        {
            DebugLogger.log("Received %s\n", buffer, offset, length);
            final SessionSubscriber subscriber = sessions.get(connectionId);
            if (subscriber != null)
            {
                subscriber.onMessage(buffer, offset, length, connectionId, sessionId, messageType);
            }
        }

        public void onDisconnect(final long connectionId)
        {
            final SessionSubscriber subscriber = sessions.remove(connectionId);
            DebugLogger.log("Library Disconnect %s\n", connectionId);
            if (subscriber != null)
            {
                subscriber.onDisconnect(connectionId);
            }
        }

        public void onError(final GatewayError errorType, final int libraryId, final String message)
        {
            if (libraryId == outboundPublication.sessionId())
            {
                FixLibrary.this.errorType = errorType;
                FixLibrary.this.errorMessage = message;
            }
        }
    });

    private void newSession(final long connectionId, final Session session)
    {
        final AuthenticationStrategy authenticationStrategy = configuration.authenticationStrategy();
        final MessageValidationStrategy validationStrategy = configuration.messageValidationStrategy();
        final SessionParser parser = new SessionParser(
            session, sessionIdStrategy, authenticationStrategy, validationStrategy);
        final SessionHandler handler = configuration.newSessionHandler().onConnect(session);
        final SessionSubscriber subscriber = new SessionSubscriber(parser, session, handler);
        sessions.put(connectionId, subscriber);
    }

    private Session initiateSession(final long connectionId)
    {
        final Object key = sessionIdStrategy.onInitiatorLogon(
            sessionConfiguration.senderCompId(), sessionConfiguration.senderSubId(),
            sessionConfiguration.senderLocationId(), sessionConfiguration.targetCompId());
        final int defaultInterval = configuration.defaultHeartbeatInterval();
        final GatewayPublication publication = outboundLibraryStreams.gatewayPublication();

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
            fixCounters.sentMsgSeqNo(connectionId),
            sessionConfiguration.username(),
            sessionConfiguration.password());
    }

    // TODO: refactor to callback
    private Session acceptSession(final long connectionId)
    {
        final GatewayPublication publication = outboundLibraryStreams.gatewayPublication();
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
            configuration.encoderBufferSize(), outboundLibraryStreams.gatewayPublication(), sessionIdStrategy,
            configuration.sessionCustomisationStrategy(), System::currentTimeMillis, connectionId);
    }

    public void close()
    {
        sessions.values().forEach(SessionSubscriber::close);
        super.close();
    }

    public boolean isConnected()
    {
        return connected;
    }

    public void onInactiveGateway()
    {
        connected = false;
    }

    public void onActiveGateway()
    {
        connected = true;
    }
}
