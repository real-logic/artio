/*
 * Copyright 2015-2016 Real Logic Ltd.
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

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.FixGatewayException;
import uk.co.real_logic.fix_gateway.LivenessDetector;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.framer.SessionIds;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.messages.ControlNotificationDecoder.SessionsDecoder;
import uk.co.real_logic.fix_gateway.protocol.*;
import uk.co.real_logic.fix_gateway.replication.ClusterableSubscription;
import uk.co.real_logic.fix_gateway.session.*;
import uk.co.real_logic.fix_gateway.timing.LibraryTimers;
import uk.co.real_logic.fix_gateway.timing.Timer;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;
import uk.co.real_logic.fix_gateway.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.validation.MessageValidationStrategy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.fix_gateway.messages.LogonStatus.LIBRARY_NOTIFICATION;
import static uk.co.real_logic.fix_gateway.messages.SessionState.ACTIVE;

final class LibraryPoller implements LibraryEndPointHandler, ProtocolHandler
{
    private final Long2ObjectHashMap<SessionSubscriber> connectionIdToSession = new Long2ObjectHashMap<>();
    private final List<Session> sessions = new ArrayList<>();
    private final List<Session> unmodifiableSessions = unmodifiableList(sessions);

    // Used when checking the consistency of the session ids
    private final LongHashSet sessionIds = new LongHashSet(SessionIds.MISSING);

    private final int libraryId;
    private final EpochClock clock;
    private final LibraryConfiguration configuration;
    private final SessionIdStrategy sessionIdStrategy;
    private final Timer sessionTimer;
    private final Timer receiveTimer;
    private final SessionExistsHandler sessionExistsHandler;
    private final IdleStrategy idleStrategy;
    private final SentPositionHandler sentPositionHandler;
    private final boolean enginesAreClustered;
    private final FixCounters fixCounters;

    private final Long2ObjectHashMap<Reply<?>> correlationIdToReply = new Long2ObjectHashMap<>();
    private final LibraryTransport transport;
    private final FixLibrary fixLibrary;

    /** Correlation Id is initialised to a random number to reduce the chance of correlation id collision. */
    private long currentCorrelationId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);

    private GatewayError errorType;
    private String errorMessage;

    // State changed upon connect/reconnect
    private LivenessDetector livenessDetector;
    private ClusterableSubscription inboundSubscription;
    private GatewayPublication outboundPublication;
    private String currentAeronChannel;

    int poll(final int fragmentLimit)
    {
        if (enginesAreClustered && livenessDetector.hasDisconnected())
        {
            connect();
        }

        final long timeInMs = clock.time();
        return inboundSubscription.controlledPoll(outboundSubscription, fragmentLimit) +
            pollSessions(timeInMs) +
            livenessDetector.poll(timeInMs) +
            checkReplies(timeInMs);
    }

    boolean isConnected()
    {
        return livenessDetector.isConnected();
    }

    int libraryId()
    {
        return libraryId;
    }

    List<Session> sessions()
    {
        return unmodifiableSessions;
    }

    void close()
    {
        connectionIdToSession.values().forEach(SessionSubscriber::close);
    }

    Reply<Session> initiate(final SessionConfiguration configuration)
    {
        requireNonNull(configuration, "configuration");

        return new InitiateSessionReply(this, latestReplyArrivalTime(), configuration);
    }

    Reply<SessionReplyStatus> releaseToGateway(final Session session)
    {
        requireNonNull(session, "session");

        return new ReleaseToGatewayReply(this, latestReplyArrivalTime(), session);
    }

    Reply<SessionReplyStatus> requestSession(final long sessionId, final int lastReceivedSequenceNumber)
    {
        return new RequestSessionReply(this, latestReplyArrivalTime(), sessionId, lastReceivedSequenceNumber);
    }

    boolean removeSession(final Session session)
    {
        return sessions.remove(session);
    }

    long saveReleaseSession(final Session session, final long correlationId)
    {
        return outboundPublication.saveReleaseSession(
            libraryId,
            session.connectionId(),
            correlationId,
            session.state(),
            session.heartbeatIntervalInMs(),
            session.lastSentMsgSeqNum(),
            session.lastReceivedMsgSeqNum(),
            session.username(),
            session.password());
    }

    long saveInitiateConnection(
        final String host,
        final int port,
        final long correlationId,
        final SessionConfiguration configuration)
    {
        return outboundPublication.saveInitiateConnection(
            libraryId,
            host,
            port,
            configuration.senderCompId(),
            configuration.senderSubId(),
            configuration.senderLocationId(),
            configuration.targetCompId(),
            configuration.sequenceNumberType(),
            configuration.initialSequenceNumber(),
            configuration.username(),
            configuration.password(),
            this.configuration.defaultHeartbeatIntervalInS(),
            correlationId);
    }

    long saveRequestSession(final long sessionId, final long correlationId, final int lastReceivedSequenceNumber)
    {
        return outboundPublication.saveRequestSession(
            libraryId, sessionId, correlationId, lastReceivedSequenceNumber);
    }

    LibraryPoller(
        final LibraryConfiguration configuration,
        final LibraryTimers timers,
        final FixCounters fixCounters,
        final LibraryTransport transport,
        final FixLibrary fixLibrary)
    {
        this.libraryId = configuration.libraryId();
        this.fixCounters = fixCounters;
        this.transport = transport;
        this.fixLibrary = fixLibrary;

        currentAeronChannel = configuration.libraryAeronChannels().get(0);
        sessionTimer = timers.sessionTimer();
        receiveTimer = timers.receiveTimer();

        this.configuration = configuration;
        this.sessionIdStrategy = configuration.sessionIdStrategy();
        sessionExistsHandler = configuration.sessionExistsHandler();
        idleStrategy = configuration.libraryIdleStrategy();
        sentPositionHandler = configuration.sentPositionHandler();
        clock = new SystemEpochClock();
        enginesAreClustered = configuration.libraryAeronChannels().size() > 1;
    }

    void connect()
    {
        connect(configuration.reconnectAttempts());
    }

    private void connect(final int reconnectAttempts)
    {
        transport.initStreams(currentAeronChannel);
        inboundSubscription = transport.inboundSubscription();
        outboundPublication = transport.outboundPublication();

        livenessDetector = LivenessDetector.forLibrary(
            outboundPublication,
            libraryId,
            configuration.replyTimeoutInMs());

        try
        {
            sendLibraryConnect();

            final String currentAeronChannel = this.currentAeronChannel;
            final long connectResendTimeout = configuration.replyTimeoutInMs() / 4;
            final long latestReplyArrivalTime = latestReplyArrivalTime();
            long latestConnectResentTime = clock.time() + connectResendTimeout;
            while (!livenessDetector.isConnected() && errorType == null)
            {
                final int workCount = poll(1);

                final long time = clock.time();
                if (time > latestReplyArrivalTime)
                {
                    if (reconnectAttempts == 0)
                    {
                        throw new IllegalStateException(String.format(
                            "Failed to receive a reply from the engine within %dms, are you sure its running?",
                            this.configuration.replyTimeoutInMs()));
                    }

                    attemptNextEngine();
                    connect(reconnectAttempts - 1);
                    return;
                }

                if (time > latestConnectResentTime)
                {
                    sendLibraryConnect();

                    latestConnectResentTime = time + connectResendTimeout;
                }

                if (!Objects.equals(currentAeronChannel, this.currentAeronChannel))
                {
                    connect(reconnectAttempts - 1);
                    return;
                }

                idleStrategy.idle(workCount);
            }

            if (errorType != null)
            {
                connectError(errorType.toString());
            }
        }
        catch (Exception e)
        {
            // We won't be returning an instance of ourselves to callers in the connect,
            // so we must clean up after ourselves
            try
            {
                close();
            }
            catch (Exception closeException)
            {
                e.addSuppressed(closeException);
            }

            LangUtil.rethrowUnchecked(e);
        }
    }

    private LibraryPoller connectError(final String message)
    {
        throw new FixGatewayException(String.format(
            "Unable to connect to engine: %s", message
        ));
    }

    private long latestReplyArrivalTime()
    {
        return clock.time() + configuration.replyTimeoutInMs();
    }

    private int checkReplies(final long timeInMs)
    {
        if (correlationIdToReply.isEmpty())
        {
            return 0;
        }

        int count = 0;
        final Iterator<Reply<?>> iterator = correlationIdToReply.values().iterator();
        while (iterator.hasNext())
        {
            final Reply<?> reply = iterator.next();
            if (reply.poll(timeInMs))
            {
                iterator.remove();
                count++;
            }
        }
        return count;
    }

    private void sendLibraryConnect()
    {
        final long correlationId = ++currentCorrelationId;
        while (outboundPublication.saveLibraryConnect(libraryId, correlationId) < 0)
        {
            idleStrategy.idle();
        }
        idleStrategy.reset();
    }

    private int pollSessions(final long timeInMs)
    {
        final List<Session> sessions = this.sessions;
        int total = 0;

        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final Session session = sessions.get(i);
            total += session.poll(timeInMs);
        }

        return total;
    }

    long register(final Reply<?> reply)
    {
        final long correlationId = ++currentCorrelationId;
        correlationIdToReply.put(correlationId, reply);
        return correlationId;
    }

    private final ControlledFragmentHandler outboundSubscription =
        ProtocolSubscription.of(this, new LibraryProtocolSubscription(this));

    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();

    public Action onManageConnection(
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final ConnectionType type,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final DirectBuffer buffer,
        final int addressOffset,
        final int addressLength,
        final SessionState state,
        final int heartbeatIntervalInS,
        final long replyToId)
    {
        if (libraryId == this.libraryId)
        {
            if (type == INITIATOR)
            {
                DebugLogger.log("Init Connect: %d, %d\n", connectionId, libraryId);
                final boolean isInitiator = correlationIdToReply.get(replyToId) instanceof InitiateSessionReply;
                final InitiateSessionReply reply =
                    isInitiator ? (InitiateSessionReply) correlationIdToReply.remove(replyToId) : null;
                final Session session = initiateSession(
                    connectionId, lastSentSequenceNumber, lastReceivedSequenceNumber, state,
                    isInitiator ? reply.configuration() : null);
                newSession(connectionId, sessionId, session);
                if (isInitiator)
                {
                    reply.onComplete(session);
                }
            }
            else
            {
                DebugLogger.log("Acct Connect: %d, %d\n", connectionId, libraryId);
                asciiBuffer.wrap(buffer);
                final String address = asciiBuffer.getAscii(addressOffset, addressLength);
                final Session session = acceptSession(connectionId, address, state, heartbeatIntervalInS);
                newSession(connectionId, sessionId, session);
            }
        }

        return CONTINUE;
    }

    public Action onLogon(
        final int libraryId,
        final long connectionId,
        final long sessionId,
        int lastSentSequenceNumber,
        int lastReceivedSequenceNumber,
        final LogonStatus status,
        final String senderCompId,
        final String senderSubId,
        final String senderLocationId,
        final String targetCompId,
        final String username,
        final String password)
    {
        final boolean thisLibrary = libraryId == this.libraryId;
        if (thisLibrary && status == LogonStatus.NEW)
        {
            DebugLogger.log("Library Logon: %d, %d\n", connectionId, sessionId);
            final SessionSubscriber subscriber = connectionIdToSession.get(connectionId);
            if (subscriber != null)
            {
                final SessionState state = subscriber.session().state();
                lastSentSequenceNumber = acceptorSequenceNumber(lastSentSequenceNumber, state);
                lastReceivedSequenceNumber = acceptorSequenceNumber(lastReceivedSequenceNumber, state);
                final CompositeKey compositeKey = senderCompId.length() == 0 ? null :
                    sessionIdStrategy.onLogon(senderCompId, senderSubId, senderLocationId, targetCompId);
                subscriber.onLogon(
                    sessionId,
                    lastSentSequenceNumber,
                    lastReceivedSequenceNumber,
                    compositeKey,
                    username,
                    password);
            }
        }
        else if (libraryId == ENGINE_LIBRARY_ID || thisLibrary && status == LIBRARY_NOTIFICATION)
        {
            sessionExistsHandler.onSessionExists(
                fixLibrary,
                sessionId,
                senderCompId,
                senderSubId,
                senderLocationId,
                targetCompId,
                username,
                password
            );
        }

        return CONTINUE;
    }

    private int acceptorSequenceNumber(int lastSequenceNumber, final SessionState state)
    {
        if (!configuration.acceptorSequenceNumbersResetUponReconnect() &&
            lastSequenceNumber != SessionInfo.UNK_SESSION)
        {
            return lastSequenceNumber;
        }

        return state == ACTIVE ? 1 : 0;
    }

    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int messageType,
        final long timestamp,
        final long position)
    {
        if (libraryId == this.libraryId)
        {
            DebugLogger.log("Received %s\n", buffer, offset, length);
            DebugLogger.log("(%d)\n", libraryId);
            final SessionSubscriber subscriber = connectionIdToSession.get(connectionId);
            if (subscriber != null)
            {
                return subscriber.onMessage(
                    buffer, offset, length, libraryId, connectionId, sessionId, messageType, timestamp, position);
            }
        }

        return CONTINUE;
    }

    public Action onDisconnect(
        final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        if (libraryId == this.libraryId)
        {
            final SessionSubscriber subscriber = connectionIdToSession.remove(connectionId);
            DebugLogger.log("Library Disconnect %d, %s\n", connectionId, reason);
            if (subscriber != null)
            {
                final Action action = subscriber.onDisconnect(libraryId, reason);
                if (action != ABORT)
                {
                    final Session session = subscriber.session();
                    session.close();
                    sessions.remove(session);
                }
                return action;
            }
        }
        return CONTINUE;
    }

    public Action onError(
        final GatewayError errorType, final int libraryId, final long replyToId, final String message)
    {
        if (libraryId == this.libraryId)
        {
            final Reply<?> reply = correlationIdToReply.remove(replyToId);
            if (reply != null)
            {
                reply.onError(errorType, errorMessage);
            }
            else
            {
                this.errorType = errorType;
                this.errorMessage = message;
            }
        }

        return configuration.gatewayErrorHandler().onError(errorType, libraryId, message);
    }

    public Action onApplicationHeartbeat(final int libraryId)
    {
        if (libraryId == this.libraryId)
        {
            livenessDetector.onHeartbeat(clock.time());
        }

        return CONTINUE;
    }

    public Action onReleaseSessionReply(final long correlationId, final SessionReplyStatus status)
    {
        final ReleaseToGatewayReply reply =
            (ReleaseToGatewayReply) correlationIdToReply.remove(correlationId);
        if (reply != null)
        {
            reply.onComplete(status);
        }

        return CONTINUE;
    }

    public Action onRequestSessionReply(final long correlationId, final SessionReplyStatus status)
    {
        final RequestSessionReply reply =
            (RequestSessionReply) correlationIdToReply.remove(correlationId);
        if (reply != null)
        {
            reply.onComplete(status);
        }

        return CONTINUE;
    }

    public Action onCatchup(final int libraryId, final long connectionId, final int messageCount)
    {
        if (this.libraryId == libraryId)
        {
            final SessionSubscriber subscriber = connectionIdToSession.get(connectionId);
            if (subscriber != null)
            {
                subscriber.startCatchup(messageCount);
            }
        }

        return CONTINUE;
    }

    public Action onNewSentPosition(final int libraryId, final long position)
    {
        if (this.libraryId == libraryId)
        {
            return sentPositionHandler.onSendCompleted(position);
        }

        return CONTINUE;
    }

    public Action onNotLeader(final int libraryId, final String libraryChannel)
    {
        if (libraryId == this.libraryId)
        {
            if (libraryChannel.isEmpty())
            {
                attemptNextEngine();
            }
            else
            {
                currentAeronChannel = libraryChannel;
                DebugLogger.log("Attempting connect to leader (%s)", currentAeronChannel);
            }
        }

        return CONTINUE;
    }

    @Override
    public Action onControlNotification(final int libraryId, final SessionsDecoder sessionsDecoder)
    {
        if (libraryId == this.libraryId)
        {
            final LongHashSet sessionIds = this.sessionIds;
            final List<Session> sessions = this.sessions;

            // copy session ids.
            sessionIds.clear();
            while (sessionsDecoder.hasNext())
            {
                sessionsDecoder.next();

                sessionIds.add(sessionsDecoder.sessionId());
            }

            for (int i = 0, size = sessions.size(); i < size; i++)
            {
                final Session session = sessions.get(i);
                final long sessionId = session.id();
                if (!sessionIds.remove(sessionId))
                {
                    final SessionSubscriber subscriber = connectionIdToSession.remove(session.connectionId());
                    if (subscriber != null)
                    {
                        subscriber.onTimeout(libraryId, sessionId);
                    }
                }
            }

            // TODO: sessions that the gateway thinks you have, that you don't?
        }

        return CONTINUE;
    }

    private void attemptNextEngine()
    {
        final List<String> aeronChannels = configuration.libraryAeronChannels();
        final int nextIndex = (aeronChannels.indexOf(currentAeronChannel) + 1) % aeronChannels.size();
        currentAeronChannel = aeronChannels.get(nextIndex);
        DebugLogger.log("Attempting connect to next engine (%s) in round-robin", currentAeronChannel);
    }

    private void newSession(final long connectionId, final long sessionId, final Session session)
    {
        session.id(sessionId);
        final AuthenticationStrategy authenticationStrategy = configuration.authenticationStrategy();
        final MessageValidationStrategy validationStrategy = configuration.messageValidationStrategy();
        final SessionParser parser = new SessionParser(
            session, sessionIdStrategy, authenticationStrategy, validationStrategy);
        final SessionHandler handler = configuration.sessionAcquireHandler().onSessionAcquired(session);
        final SessionSubscriber subscriber = new SessionSubscriber(parser, session, handler,
            receiveTimer, sessionTimer);
        connectionIdToSession.put(connectionId, subscriber);
        sessions.add(session);
    }

    private Session initiateSession(
        final long connectionId,
        final int lastSequenceNumber,
        final int lastReceivedSequenceNumber,
        final SessionState state,
        final SessionConfiguration sessionConfiguration)
    {
        final int defaultInterval = configuration.defaultHeartbeatIntervalInS();
        final GatewayPublication publication = transport.outboundPublication();

        final SessionProxy sessionProxy = sessionProxy(connectionId);
        if (sessionConfiguration != null)
        {
            final CompositeKey key = sessionIdStrategy.onLogon(
                sessionConfiguration.senderCompId(), sessionConfiguration.senderSubId(),
                sessionConfiguration.senderLocationId(), sessionConfiguration.targetCompId());
            sessionProxy.setupSession(-1, key);
        }

        return new InitiatorSession(
            defaultInterval,
            connectionId,
            clock,
            sessionProxy,
            publication,
            sessionIdStrategy,
            configuration.sendingTimeWindowInMs(),
            fixCounters.receivedMsgSeqNo(connectionId),
            fixCounters.sentMsgSeqNo(connectionId),
            libraryId,
            configuration.sessionBufferSize(),
            initiatorInitialSequenceNumber(sessionConfiguration, lastSequenceNumber),
            state)
            .lastReceivedMsgSeqNum(
                initiatorInitialSequenceNumber(sessionConfiguration, lastReceivedSequenceNumber) - 1);
    }

    private int initiatorInitialSequenceNumber(
        final SessionConfiguration sessionConfiguration, final int lastSequenceNumber)
    {
        if (sessionConfiguration == null)
        {
            return 1;
        }

        if (sessionConfiguration.hasCustomInitialSequenceNumber())
        {
            return sessionConfiguration.initialSequenceNumber();
        }

        if (sessionConfiguration.sequenceNumbersPersistent() && lastSequenceNumber != SessionInfo.UNK_SESSION)
        {
            return lastSequenceNumber + 1;
        }

        return 1;
    }

    private Session acceptSession(final long connectionId,
                                  final String address,
                                  final SessionState state,
                                  final int heartbeatIntervalInS)
    {
        final GatewayPublication publication = transport.outboundPublication();
        final int split = address.lastIndexOf(':');
        final int start = address.startsWith("/") ? 1 : 0;
        final String host = address.substring(start, split);
        final int port = Integer.parseInt(address.substring(split + 1));
        final long sendingTimeWindow = configuration.sendingTimeWindowInMs();
        final AtomicCounter receivedMsgSeqNo = fixCounters.receivedMsgSeqNo(connectionId);
        final AtomicCounter sentMsgSeqNo = fixCounters.sentMsgSeqNo(connectionId);
        final int sessionBufferSize = configuration.sessionBufferSize();

        return new AcceptorSession(
            heartbeatIntervalInS,
            connectionId,
            clock,
            sessionProxy(connectionId),
            publication,
            sessionIdStrategy,
            sendingTimeWindow,
            receivedMsgSeqNo,
            sentMsgSeqNo,
            libraryId,
            sessionBufferSize,
            1,
            state)
            .address(host, port);
    }

    private SessionProxy sessionProxy(final long connectionId)
    {
        return new SessionProxy(
            configuration.encoderBufferSize(),
            transport.outboundPublication(),
            sessionIdStrategy,
            configuration.sessionCustomisationStrategy(),
            new SystemEpochClock(),
            connectionId,
            libraryId);
    }

    String currentAeronChannel()
    {
        return currentAeronChannel;
    }
}
