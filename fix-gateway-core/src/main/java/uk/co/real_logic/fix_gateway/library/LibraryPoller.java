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

import io.aeron.Subscription;
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
import uk.co.real_logic.fix_gateway.LivenessDetector;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.messages.ControlNotificationDecoder.SessionsDecoder;
import uk.co.real_logic.fix_gateway.protocol.*;
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
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static uk.co.real_logic.fix_gateway.LogTag.*;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.engine.framer.SessionContexts.MISSING_SESSION_ID;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.fix_gateway.messages.LogonStatus.LIBRARY_NOTIFICATION;

final class LibraryPoller implements LibraryEndPointHandler, ProtocolHandler, AutoCloseable
{
    private enum State
    {
        /**
         * Has connected to an engine instance
         */
        CONNECTED,

        /**
         * Currently connecting to an engine instance
         */
        CONNECTING,

        /**
         * Was explicitly closed
         */
        CLOSED
    }

    private static final long NO_CORRELATION_ID = 0;

    private final Long2ObjectHashMap<SessionSubscriber> connectionIdToSession = new Long2ObjectHashMap<>();
    private final ArrayList<Session> sessions = new ArrayList<>();
    private final List<Session> unmodifiableSessions = unmodifiableList(sessions);

    // Used when checking the consistency of the session ids
    private final LongHashSet sessionIds = new LongHashSet(MISSING_SESSION_ID);

    private final SessionAccessor accessor = new SessionAccessor(LibraryPoller.class);

    // Uniquely identifies library session
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

    private final Long2ObjectHashMap<LibraryReply<?>> correlationIdToReply = new Long2ObjectHashMap<>();
    private final LibraryTransport transport;
    private final FixLibrary fixLibrary;
    private final Runnable onDisconnectFunc = this::onDisconnect;

    /**
     * Correlation Id is initialised to a random number to reduce the chance of correlation id collision.
     */
    private long currentCorrelationId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);

    private String errorMessage;

    private State state = State.CONNECTING;

    // State changed upon connect/reconnect
    private LivenessDetector livenessDetector;
    private Subscription inboundSubscription;
    private GatewayPublication outboundPublication;
    private String currentAeronChannel;
    private long nextAttemptTime;
    private long completeFailureTime;

    // Combined with Library Id, uniquely identifies library connection
    private long connectCorrelationId = NO_CORRELATION_ID;

    LibraryPoller(
        final LibraryConfiguration configuration,
        final LibraryTimers timers,
        final FixCounters fixCounters,
        final LibraryTransport transport,
        final FixLibrary fixLibrary,
        final EpochClock clock)
    {
        this.libraryId = configuration.libraryId();
        this.fixCounters = fixCounters;
        this.transport = transport;
        this.fixLibrary = fixLibrary;

        sessionTimer = timers.sessionTimer();
        receiveTimer = timers.receiveTimer();

        this.configuration = configuration;
        this.sessionIdStrategy = configuration.sessionIdStrategy();
        sessionExistsHandler = configuration.sessionExistsHandler();
        idleStrategy = configuration.libraryIdleStrategy();
        sentPositionHandler = configuration.sentPositionHandler();
        this.clock = clock;
        enginesAreClustered = configuration.libraryAeronChannels().size() > 1;
    }

    boolean isConnected()
    {
        return state == State.CONNECTED;
    }

    boolean isClosed()
    {
        return state == State.CLOSED;
    }

    int libraryId()
    {
        return libraryId;
    }

    long connectCorrelationId()
    {
        return connectCorrelationId;
    }

    List<Session> sessions()
    {
        return unmodifiableSessions;
    }

    Reply<Session> initiate(final SessionConfiguration configuration)
    {
        requireNonNull(configuration, "configuration");

        return new InitiateSessionReply(this, timeInMs() + configuration.timeoutInMs(), configuration);
    }

    Reply<SessionReplyStatus> releaseToGateway(final Session session, final long timeoutInMs)
    {
        requireNonNull(session, "session");

        return new ReleaseToGatewayReply(this, timeInMs() + timeoutInMs, session);
    }

    Reply<SessionReplyStatus> requestSession(
        final long sessionId,
        final int lastReceivedSequenceNumber,
        final int sequenceIndex,
        final long timeoutInMs)
    {
        return new RequestSessionReply(
            this, timeInMs() + timeoutInMs, sessionId, lastReceivedSequenceNumber, sequenceIndex);
    }

    void disableSession(final Session session)
    {
        sessions.remove(session);
        accessor.disable(session);
    }

    long saveReleaseSession(final Session session, final long correlationId)
    {
        checkState();

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
        checkState();

        return outboundPublication.saveInitiateConnection(
            libraryId,
            host,
            port,
            configuration.senderCompId(),
            configuration.senderSubId(),
            configuration.senderLocationId(),
            configuration.targetCompId(),
            configuration.targetSubId(),
            configuration.targetLocationId(),
            configuration.sequenceNumberType(),
            configuration.resetSeqNum(),
            configuration.initialSequenceNumber(),
            configuration.username(),
            configuration.password(),
            this.configuration.defaultHeartbeatIntervalInS(),
            correlationId);
    }

    long saveRequestSession(
        final long sessionId,
        final long correlationId,
        final int lastReceivedSequenceNumber,
        final int sequenceIndex)
    {
        checkState();

        return outboundPublication.saveRequestSession(
            libraryId, sessionId, correlationId, lastReceivedSequenceNumber, sequenceIndex);
    }

    int poll(final int fragmentLimit)
    {
        final long timeInMs = timeInMs();
        switch (state)
        {
            case CONNECTING:
                nextConnectingStep(timeInMs);

                return pollWithoutReconnect(timeInMs, fragmentLimit);

            case CONNECTED:
                if (livenessDetector.hasDisconnected())
                {
                    connect(timeInMs);
                }

                return pollWithoutReconnect(timeInMs, fragmentLimit);

            case CLOSED:
            default:
                return 0;
        }
    }

    private int pollWithoutReconnect(final long timeInMs, final int fragmentLimit)
    {
        final int messagesRead = inboundSubscription.controlledPoll(outboundSubscription, fragmentLimit);
        return messagesRead +
            pollSessions(timeInMs) +
            livenessDetector.poll(timeInMs) +
            checkReplies(timeInMs);
    }

    // -----------------------------------------------------------------------
    //                     BEGIN CONNECTION LOGIC
    // -----------------------------------------------------------------------

    void connect()
    {
        connect(timeInMs());
    }

    // Called when you first connect or try to start a reconnect attempt
    private void connect(final long timeInMs)
    {
        try
        {
            state = State.CONNECTING;
            currentAeronChannel = configuration.libraryAeronChannels().get(0);
            DebugLogger.log(LIBRARY_CONNECT, "Attempting to connect to %s\n", currentAeronChannel);

            initStreams();
            newLivenessDetector();

            completeFailureTime = configuration.replyTimeoutInMs() + timeInMs;

            sendLibraryConnect(timeInMs);
        }
        catch (final Exception ex)
        {
            // We won't be returning an instance of ourselves to callers in the connect,
            // so we must clean up after ourselves
            try
            {
                closeWithParent();
            }
            catch (final Exception closeException)
            {
                ex.addSuppressed(closeException);
            }

            LangUtil.rethrowUnchecked(ex);
        }
    }

    // NB: not a reconnect, an nth attempt at a connect
    private void nextConnectingStep(final long timeInMs)
    {
        if (livenessDetector.isConnected())
        {
            state = State.CONNECTED;
            onConnect();
        }
        else if (timeInMs > completeFailureTime)
        {
            closeWithParent();
            throw illegalStateDueToFailingToConnect();
        }
        else if (timeInMs > nextAttemptTime)
        {
            attemptNextEngine();

            connectToNewEngine(timeInMs);
        }
    }

    private void connectToNewEngine(final long timeInMs)
    {
        initStreams();

        sendLibraryConnect(timeInMs);
    }

    private void attemptNextEngine()
    {
        if (enginesAreClustered)
        {
            final List<String> aeronChannels = configuration.libraryAeronChannels();
            final int nextIndex = (aeronChannels.indexOf(currentAeronChannel) + 1) % aeronChannels.size();
            currentAeronChannel = aeronChannels.get(nextIndex);
            DebugLogger.log(
                LIBRARY_CONNECT, "Attempting connect to next engine (%s) in round-robin\n", currentAeronChannel);
        }
    }

    private IllegalStateException illegalStateDueToFailingToConnect()
    {
        return new IllegalStateException(String.format(
            "Failed to receive a reply from the engine within %dms, are you sure its running?",
            this.configuration.replyTimeoutInMs()));
    }

    private void initStreams()
    {
        if (enginesAreClustered || isFirstConnect())
        {
            transport.initStreams(currentAeronChannel);
            inboundSubscription = transport.inboundSubscription();
            outboundPublication = transport.outboundPublication();
        }
    }

    private void newLivenessDetector()
    {
        livenessDetector = LivenessDetector.forLibrary(
            outboundPublication,
            libraryId,
            configuration.replyTimeoutInMs(),
            onDisconnectFunc);
    }

    private boolean isFirstConnect()
    {
        return !transport.isReconnect();
    }

    // return true if sent, false otherwise
    private void sendLibraryConnect(final long timeInMs)
    {
        checkState();

        try
        {
            final long correlationId = ++currentCorrelationId;
            final int maxClaimAttempts = configuration.outboundMaxClaimAttempts();
            long position = Long.MIN_VALUE;
            for (int i = 0; i < maxClaimAttempts && position < 0; i++)
            {
                position = outboundPublication.saveLibraryConnect(libraryId, correlationId);
                if (position >= 0)
                {
                    break;
                }

                idleStrategy.idle();
            }
            idleStrategy.reset();

            if (position > 0)
            {
                this.connectCorrelationId = correlationId;
                final int reconnectAttempts = configuration.reconnectAttempts();
                nextAttemptTime = (configuration.replyTimeoutInMs() / reconnectAttempts) + timeInMs;
            }
            else
            {
                nextAttemptTime = timeInMs;
            }
        }
        catch (final NotConnectedException e)
        {
            nextAttemptTime = timeInMs;
        }
    }

    private void onConnect()
    {
        DebugLogger.log(LIBRARY_CONNECT, "Connected to [%s]\n", currentAeronChannel);
        configuration.libraryConnectHandler().onConnect(fixLibrary);
        setLibraryConnected(true);
    }

    private void onDisconnect()
    {
        DebugLogger.log(LIBRARY_CONNECT, "Disconnected from [%s]\n", currentAeronChannel);
        configuration.libraryConnectHandler().onDisconnect(fixLibrary);
        setLibraryConnected(false);

        connect();
    }

    private void setLibraryConnected(final boolean libraryConnected)
    {
        final ArrayList<Session> sessions = this.sessions;
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final Session session = sessions.get(i);
            accessor.libraryConnected(session, libraryConnected);
        }
    }

    String currentAeronChannel()
    {
        return currentAeronChannel;
    }

    // -----------------------------------------------------------------------
    //                     END CONNECTION LOGIC
    // -----------------------------------------------------------------------

    private int pollSessions(final long timeInMs)
    {
        final ArrayList<Session> sessions = this.sessions;
        int total = 0;

        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final Session session = sessions.get(i);
            total += session.poll(timeInMs);
        }

        return total;
    }

    private long timeInMs()
    {
        return clock.time();
    }

    private int checkReplies(final long timeInMs)
    {
        if (correlationIdToReply.isEmpty())
        {
            return 0;
        }

        int count = 0;
        final Iterator<LibraryReply<?>> iterator = correlationIdToReply.values().iterator();
        while (iterator.hasNext())
        {
            final LibraryReply<?> reply = iterator.next();
            if (reply.poll(timeInMs))
            {
                iterator.remove();
                count++;
            }
        }

        return count;
    }

    long register(final LibraryReply<?> reply)
    {
        final long correlationId = ++currentCorrelationId;
        correlationIdToReply.put(correlationId, reply);
        return correlationId;
    }

    // -----------------------------------------------------------------------
    //                     BEGIN EVENT HANDLERS
    // -----------------------------------------------------------------------

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
        final long replyToId,
        final int sequenceIndex)
    {
        if (libraryId == this.libraryId)
        {
            if (type == INITIATOR)
            {
                DebugLogger.log(FIX_MESSAGE, "Init Connect: %d, %d\n", connectionId, libraryId);
                final boolean isInitiator = correlationIdToReply.get(replyToId) instanceof InitiateSessionReply;
                final InitiateSessionReply reply =
                    isInitiator ? (InitiateSessionReply) correlationIdToReply.remove(replyToId) : null;
                final Session session = initiateSession(
                    connectionId, lastSentSequenceNumber, lastReceivedSequenceNumber, state,
                    isInitiator ? reply.configuration() : null, sequenceIndex);
                newSession(connectionId, sessionId, session);
                if (isInitiator)
                {
                    reply.onComplete(session);
                }
            }
            else
            {
                DebugLogger.log(FIX_MESSAGE, "Acct Connect: %d, %d\n", connectionId, libraryId);
                asciiBuffer.wrap(buffer);
                final String address = asciiBuffer.getAscii(addressOffset, addressLength);
                final Session session = acceptSession(
                    connectionId, address, state, heartbeatIntervalInS, sequenceIndex);
                newSession(connectionId, sessionId, session);
            }
        }

        return CONTINUE;
    }

    public Action onLogon(
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final LogonStatus status,
        final String localCompId,
        final String localSubId,
        final String localLocationId,
        final String remoteCompId,
        final String remoteSubId,
        final String remoteLocationId,
        final String username,
        final String password)
    {

        final boolean thisLibrary = libraryId == this.libraryId;
        if (thisLibrary && status == LogonStatus.NEW)
        {
            DebugLogger.log(FIX_MESSAGE, "Library Logon: %d, %d\n", connectionId, sessionId);
            final SessionSubscriber subscriber = connectionIdToSession.get(connectionId);
            if (subscriber != null)
            {
                final CompositeKey compositeKey = localCompId.length() == 0 ? null :
                    sessionIdStrategy.onInitiateLogon(
                        localCompId,
                        localSubId,
                        localLocationId,
                        remoteCompId,
                        remoteSubId,
                        remoteLocationId);
                final SessionHandler handler =
                    configuration.sessionAcquireHandler().onSessionAcquired(subscriber.session());
                subscriber.onLogon(
                    sessionId,
                    lastSentSequenceNumber,
                    lastReceivedSequenceNumber,
                    compositeKey,
                    username,
                    password,
                    handler);
            }
        }
        else if (libraryId == ENGINE_LIBRARY_ID || thisLibrary && status == LIBRARY_NOTIFICATION)
        {
            sessionExistsHandler.onSessionExists(
                fixLibrary,
                sessionId,
                localCompId,
                localSubId,
                localLocationId,
                remoteCompId,
                remoteSubId,
                remoteLocationId,
                username,
                password);
        }

        return CONTINUE;
    }

    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int sequenceIndex,
        final int messageType,
        final long timestamp,
        final MessageStatus status,
        final long position)
    {
        if (libraryId == this.libraryId)
        {
            DebugLogger.log(FIX_MESSAGE, "(%d) Received %s \n", libraryId, buffer, offset, length);
            final SessionSubscriber subscriber = connectionIdToSession.get(connectionId);
            if (subscriber != null)
            {
                return subscriber.onMessage(
                    buffer,
                    offset,
                    length,
                    libraryId,
                    sessionId,
                    sequenceIndex,
                    messageType,
                    timestamp,
                    status,
                    position);
            }
        }

        return CONTINUE;
    }

    public Action onDisconnect(
        final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        DebugLogger.log(FIX_MESSAGE, "%2$d: Library Disconnect %3$d, %1$s\n", reason, libraryId, connectionId);
        if (libraryId == this.libraryId)
        {
            final SessionSubscriber subscriber = connectionIdToSession.remove(connectionId);
            if (subscriber != null)
            {
                final Action action = subscriber.onDisconnect(libraryId, reason);
                if (action == ABORT)
                {
                    // If we abort the action then we should ensure that it can be processed when
                    // re-run.
                    connectionIdToSession.put(connectionId, subscriber);
                }
                else
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
        final int libraryId,
        final GatewayError errorType,
        final long replyToId,
        final String message)
    {
        if (libraryId == this.libraryId)
        {
            final LibraryReply<?> reply = correlationIdToReply.remove(replyToId);
            if (reply != null)
            {
                reply.onError(errorType, errorMessage);
            }
            else
            {
                // case of a connect error
                this.errorMessage = message;
            }
        }

        return configuration.gatewayErrorHandler().onError(errorType, libraryId, message);
    }

    public Action onApplicationHeartbeat(final int libraryId)
    {
        if (libraryId == this.libraryId)
        {
            final long timeInMs = timeInMs();
            DebugLogger.log(
                APPLICATION_HEARTBEAT, "%d: Received Heartbeat from engine at timeInMs %d\n", libraryId, timeInMs);
            livenessDetector.onHeartbeat(timeInMs);
        }

        return CONTINUE;
    }

    public Action onReleaseSessionReply(final int libraryId, final long replyToId, final SessionReplyStatus status)
    {
        final ReleaseToGatewayReply reply =
            (ReleaseToGatewayReply) correlationIdToReply.remove(replyToId);
        if (reply != null)
        {
            reply.onComplete(status);
        }

        return CONTINUE;
    }

    public Action onRequestSessionReply(final int toId, final long replyToId, final SessionReplyStatus status)
    {
        final RequestSessionReply reply = (RequestSessionReply) correlationIdToReply.remove(replyToId);
        if (reply != null)
        {
            reply.onComplete(status);
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

    public Action onNotLeader(final int libraryId, final long replyToId, final String libraryChannel)
    {
        if (libraryId == this.libraryId && replyToId >= connectCorrelationId)
        {
            if (libraryChannel.isEmpty())
            {
                attemptNextEngine();
            }
            else
            {
                currentAeronChannel = libraryChannel;
                DebugLogger.log(LIBRARY_CONNECT, "Attempting connect to (%s) claimed leader\n", currentAeronChannel);
            }

            connectToNewEngine(timeInMs());
        }

        return CONTINUE;
    }

    @Override
    public Action onControlNotification(final int libraryId, final SessionsDecoder sessionsDecoder)
    {
        if (libraryId == this.libraryId)
        {
            final long timeInMs = timeInMs();
            livenessDetector.onHeartbeat(timeInMs);
            DebugLogger.log(
                LIBRARY_CONNECT, "%d: Received Control Notification from engine at timeInMs %d\n", libraryId, timeInMs);

            final LongHashSet sessionIds = this.sessionIds;
            final ArrayList<Session> sessions = this.sessions;

            // copy session ids.
            sessionIds.clear();
            while (sessionsDecoder.hasNext())
            {
                sessionsDecoder.next();
                sessionIds.add(sessionsDecoder.sessionId());
            }

            for (int i = 0, size = sessions.size(); i < size; )
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
                    session.close();
                    sessions.remove(i);
                    size--;
                }
                else
                {
                    i++;
                }
            }

            // sessions that the gateway thinks you have, that you don't
            if (!sessionIds.isEmpty())
            {
                configuration
                    .gatewayErrorHandler()
                    .onError(GatewayError.UNKNOWN_SESSION, libraryId,
                        String.format("The gateway thinks that we own the following session ids: %s", sessionIds));
            }
        }

        return CONTINUE;
    }

    // -----------------------------------------------------------------------
    //                     END EVENT HANDLERS
    // -----------------------------------------------------------------------

    private void newSession(final long connectionId, final long sessionId, final Session session)
    {
        session.id(sessionId);
        final AuthenticationStrategy authenticationStrategy = configuration.authenticationStrategy();
        final MessageValidationStrategy validationStrategy = configuration.messageValidationStrategy();
        final SessionParser parser = new SessionParser(
            session, sessionIdStrategy, authenticationStrategy, validationStrategy);
        final SessionSubscriber subscriber = new SessionSubscriber(parser, session, receiveTimer, sessionTimer);
        connectionIdToSession.put(connectionId, subscriber);
        sessions.add(session);
    }

    private Session initiateSession(
        final long connectionId,
        final int lastSequenceNumber,
        final int lastReceivedSequenceNumber,
        final SessionState state,
        final SessionConfiguration sessionConfiguration,
        final int sequenceIndex)
    {
        final int defaultInterval = configuration.defaultHeartbeatIntervalInS();
        final GatewayPublication publication = transport.outboundPublication();

        final SessionProxy sessionProxy = sessionProxy(connectionId);
        final Session session = new InitiatorSession(
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
            sequenceIndex,
            state,
            sessionConfiguration != null && sessionConfiguration.resetSeqNum(),
            configuration.reasonableTransmissionTimeInMs())
            .lastReceivedMsgSeqNum(
                initiatorInitialSequenceNumber(sessionConfiguration, lastReceivedSequenceNumber) - 1);

        if (sessionConfiguration != null)
        {
            final CompositeKey key = sessionIdStrategy.onInitiateLogon(
                sessionConfiguration.senderCompId(),
                sessionConfiguration.senderSubId(),
                sessionConfiguration.senderLocationId(),
                sessionConfiguration.targetCompId(),
                sessionConfiguration.targetSubId(),
                sessionConfiguration.targetLocationId());
            sessionProxy.setupSession(-1, key);

            session
                .username(sessionConfiguration.username())
                .password(sessionConfiguration.password());
        }

        return session;
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

    private Session acceptSession(
        final long connectionId,
        final String address,
        final SessionState state,
        final int heartbeatIntervalInS,
        final int sequenceIndex)
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
            sequenceIndex,
            state,
            configuration.reasonableTransmissionTimeInMs())
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

    private void checkState()
    {
        // TODO: ban connecting from everything but library connect
        if (state == State.CLOSED)
        {
            throw new IllegalStateException("Library has been closed");
        }
    }

    private void closeWithParent()
    {
        try
        {
            fixLibrary.close();
        }
        finally
        {
            close();
        }
    }

    public void close()
    {
        if (state != State.CLOSED)
        {
            connectionIdToSession.values().forEach(subscriber -> accessor.disable(subscriber.session()));
            state = State.CLOSED;
        }
    }
}
