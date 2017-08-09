/*
 * Copyright 2015-2017 Real Logic Ltd.
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

import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.*;
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

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static java.util.Objects.requireNonNull;
import static uk.co.real_logic.fix_gateway.LogTag.*;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.fix_gateway.messages.LogonStatus.LIBRARY_NOTIFICATION;

final class LibraryPoller implements LibraryEndPointHandler, ProtocolHandler, AutoCloseable
{
    /**
     * Has connected to an engine instance
     */
    private static final int CONNECTED = 0;

    /**
     * Currently connecting to an engine instance
     */
    private static final int ATTEMPT_CONNECT = 1;

    /**
     * Currently connecting to an engine instance
     */
    private static final int CONNECTING = 2;

    /**
     * Currently connecting to an engine instance
     */
    private static final int ATTEMPT_CURRENT_NODE = 3;

    /**
     * Was explicitly closed
     */
    private static final int CLOSED = 4;

    private static final long NO_CORRELATION_ID = 0;

    private final Long2ObjectHashMap<SessionSubscriber> connectionIdToSession = new Long2ObjectHashMap<>();
    private Session[] sessions = new Session[0];
    private final List<Session> unmodifiableSessions = new AbstractList<Session>()
    {
        public Session get(final int index)
        {
            return sessions[index];
        }

        public int size()
        {
            return sessions.length;
        }
    };

    // Used when checking the consistency of the session ids
    private final LongHashSet sessionIds = new LongHashSet();

    private final SessionAccessor accessor = new SessionAccessor(LibraryPoller.class);

    // Uniquely identifies library session
    private final int libraryId;
    private final EpochClock clock;
    private final LibraryConfiguration configuration;
    private final SessionIdStrategy sessionIdStrategy;
    private final Timer sessionTimer;
    private final Timer receiveTimer;
    private final SessionExistsHandler sessionExistsHandler;
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

    private int state = CONNECTING;

    // State changed upon connect/reconnect
    private LivenessDetector livenessDetector;
    private Subscription inboundSubscription;
    private GatewayPublication outboundPublication;
    private String currentAeronChannel;
    private long nextSendLibraryConnectTime;
    private long nextEngineAttemptTime;

    // Combined with Library Id, uniquely identifies library connection
    private long connectCorrelationId = NO_CORRELATION_ID;
    private volatile Throwable remoteThrowable;

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

        this.sessionTimer = timers.sessionTimer();
        this.receiveTimer = timers.receiveTimer();

        this.configuration = configuration;
        this.sessionIdStrategy = configuration.sessionIdStrategy();
        this.sessionExistsHandler = configuration.sessionExistsHandler();
        this.sentPositionHandler = configuration.sentPositionHandler();
        this.clock = clock;
        this.enginesAreClustered = configuration.libraryAeronChannels().size() > 1;
    }

    boolean isConnected()
    {
        return state == CONNECTED;
    }

    boolean isClosed()
    {
        return state == CLOSED;
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
        sessions = ArrayUtil.remove(sessions, session);
        accessor.disable(session);
    }

    long saveReleaseSession(final Session session, final long correlationId)
    {
        checkState();

        return outboundPublication.saveReleaseSession(
            libraryId,
            session.connectionId(),
            session.id(),
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

        if (null != remoteThrowable)
        {
            LangUtil.rethrowUnchecked(remoteThrowable);
        }

        switch (state)
        {
            case CONNECTED:
                return pollWithoutReconnect(timeInMs, fragmentLimit);

            case ATTEMPT_CONNECT:
                startConnecting();
                return pollWithoutReconnect(timeInMs, fragmentLimit);

            case CONNECTING:
                nextConnectingStep(timeInMs);
                return pollWithoutReconnect(timeInMs, fragmentLimit);

            case ATTEMPT_CURRENT_NODE:
                connectToNewEngine(timeInMs);
                state = CONNECTING;
                return pollWithoutReconnect(timeInMs, fragmentLimit);

            case CLOSED:
            default:
                return 0;
        }
    }

    private int pollWithoutReconnect(final long timeInMs, final int fragmentLimit)
    {
        int operations = 0;
        operations += inboundSubscription.controlledPoll(outboundSubscription, fragmentLimit);
        operations += livenessDetector.poll(timeInMs);
        operations += pollSessions(timeInMs);
        operations += checkReplies(timeInMs);
        return operations;
    }

    void postExceptionToLibraryThread(final Throwable t)
    {
        this.remoteThrowable = t;
    }

    // -----------------------------------------------------------------------
    //                     BEGIN CONNECTION LOGIC
    // -----------------------------------------------------------------------

    void startConnecting()
    {
        startConnecting(timeInMs());
    }

    // Called when you first connect or try to start a reconnect attempt
    private void startConnecting(final long timeInMs)
    {
        try
        {
            state = CONNECTING;
            currentAeronChannel = configuration.libraryAeronChannels().get(0);
            DebugLogger.log(
                LIBRARY_CONNECT,
                "%d: Attempting to connect to %s%n",
                libraryId,
                currentAeronChannel);

            initStreams();
            newLivenessDetector();
            resetNextEngineTimer(timeInMs);

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
        if (timeInMs > nextEngineAttemptTime)
        {
            attemptNextEngine();

            connectToNewEngine(timeInMs);
        }
        else if (timeInMs > nextSendLibraryConnectTime)
        {
            sendLibraryConnect(timeInMs);
        }
    }

    private void connectToNewEngine(final long timeInMs)
    {
        initStreams();
        newLivenessDetector();

        sendLibraryConnect(timeInMs);

        resetNextEngineTimer(timeInMs);
    }

    private void attemptNextEngine()
    {
        if (enginesAreClustered)
        {
            final List<String> aeronChannels = configuration.libraryAeronChannels();
            final int nextIndex = (aeronChannels.indexOf(currentAeronChannel) + 1) % aeronChannels.size();
            currentAeronChannel = aeronChannels.get(nextIndex);
            DebugLogger.log(
                LIBRARY_CONNECT,
                "%d: Attempting connect to next engine (%s) in round-robin%n",
                libraryId,
                currentAeronChannel);
        }
    }

    private void resetNextEngineTimer(final long timeInMs)
    {
        nextEngineAttemptTime = configuration.replyTimeoutInMs() + timeInMs;
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
            if (outboundPublication.saveLibraryConnect(libraryId, configuration.libraryName(), correlationId) < 0)
            {
                connectToNextEngineNow(timeInMs);
            }
            else
            {
                this.connectCorrelationId = correlationId;
                nextSendLibraryConnectTime = configuration.connectAttemptTimeoutInMs() + timeInMs;
            }
        }
        catch (final NotConnectedException e)
        {
            connectToNextEngineNow(timeInMs);
        }
    }

    private void connectToNextEngineNow(final long timeInMs)
    {
        nextEngineAttemptTime = timeInMs;
    }

    private void onConnect()
    {
        DebugLogger.log(
            LIBRARY_CONNECT,
            "%d: Connected to [%s]%n",
            libraryId,
            currentAeronChannel);
        configuration.libraryConnectHandler().onConnect(fixLibrary);
        setLibraryConnected(true);
    }

    private void onDisconnect()
    {
        DebugLogger.log(
            LIBRARY_CONNECT,
            "%d: Disconnected from [%s]%n",
            libraryId,
            currentAeronChannel);
        configuration.libraryConnectHandler().onDisconnect(fixLibrary);
        setLibraryConnected(false);

        state = ATTEMPT_CONNECT;
    }

    private void setLibraryConnected(final boolean libraryConnected)
    {
        final Session[] sessions = this.sessions;
        for (int i = 0, size = sessions.length; i < size; i++)
        {
            final Session session = sessions[i];
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
        final Session[] sessions = this.sessions;
        int total = 0;

        for (int i = 0, size = sessions.length; i < size; i++)
        {
            final Session session = sessions[i];
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
        new ControlledFragmentAssembler(
            ProtocolSubscription.of(this, new LibraryProtocolSubscription(this)));

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
                DebugLogger.log(FIX_MESSAGE, "Init Connect: %d, %d%n", connectionId, libraryId);
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
                DebugLogger.log(FIX_MESSAGE, "Acct Connect: %d, %d%n", connectionId, libraryId);
                asciiBuffer.wrap(buffer);
                final String address = asciiBuffer.getAscii(addressOffset, addressLength);
                final Session session = acceptSession(
                    connectionId, address, state, heartbeatIntervalInS, sequenceIndex);
                newSession(connectionId, sessionId, session);
            }
        }

        return CONTINUE;
    }

    public Action onSessionExists(
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final LogonStatus logonstatus,
        final boolean isSlow,
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
        if (thisLibrary && logonstatus == LogonStatus.NEW)
        {
            DebugLogger.log(
                GATEWAY_MESSAGE,
                "onSessionExists: conn=%d, sess=%d, sentSeqNo=%d, recvSeqNo=%d%n",
                connectionId,
                sessionId,
                lastSentSequenceNumber,
                lastReceivedSequenceNumber);
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

                subscriber.onLogon(
                    sessionId,
                    lastSentSequenceNumber,
                    lastReceivedSequenceNumber,
                    compositeKey,
                    username,
                    password);
                final SessionHandler handler =
                    configuration.sessionAcquireHandler().onSessionAcquired(subscriber.session(), isSlow);
                subscriber.handler(handler);
            }
        }
        else if (libraryId == ENGINE_LIBRARY_ID || thisLibrary && logonstatus == LIBRARY_NOTIFICATION)
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
            DebugLogger.log(FIX_MESSAGE, "(%d) Received %s %n", libraryId, buffer, offset, length);
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
        DebugLogger.log(GATEWAY_MESSAGE, "%2$d: Library Disconnect %3$d, %1$s%n", reason, libraryId, connectionId);
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
                    sessions = ArrayUtil.remove(sessions, session);
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
                APPLICATION_HEARTBEAT, "%d: Received Heartbeat from engine at timeInMs %d%n", libraryId, timeInMs);
            livenessDetector.onHeartbeat(timeInMs);

            if (!isConnected() && livenessDetector.isConnected())
            {
                state = CONNECTED;
                onConnect();
            }
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
            final long timeInMs = timeInMs();
            if (libraryChannel.isEmpty())
            {
                connectToNextEngineNow(timeInMs);
            }
            else
            {
                DebugLogger.log(
                    LIBRARY_CONNECT,
                    "%d: Attempting connect to (%s) claimed leader%n",
                    libraryId,
                    currentAeronChannel);

                currentAeronChannel = libraryChannel;
                state = ATTEMPT_CURRENT_NODE;
            }

            return BREAK;
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
            state = CONNECTED;
            DebugLogger.log(
                LIBRARY_CONNECT,
                "%d: Received Control Notification from engine at timeInMs %d%n",
                libraryId,
                timeInMs);

            final LongHashSet sessionIds = this.sessionIds;
            Session[] sessions = this.sessions;

            // copy session ids.
            sessionIds.clear();
            while (sessionsDecoder.hasNext())
            {
                sessionsDecoder.next();
                sessionIds.add(sessionsDecoder.sessionId());
            }

            for (int i = 0, size = sessions.length; i < size; i++)
            {
                final Session session = sessions[i];
                final long sessionId = session.id();
                if (!sessionIds.remove(sessionId))
                {
                    final SessionSubscriber subscriber = connectionIdToSession.remove(session.connectionId());
                    if (subscriber != null)
                    {
                        subscriber.onTimeout(libraryId);
                    }
                    session.close();
                    sessions = ArrayUtil.remove(sessions, i);
                    size--;
                }
                else
                {
                    i++;
                }
            }
            this.sessions = sessions;

            // sessions that the gateway thinks you have, that you don't
            if (!sessionIds.isEmpty())
            {
                configuration
                    .gatewayErrorHandler()
                    .onError(GatewayError.UNKNOWN_SESSION, libraryId,
                        String.format("The gateway thinks that we own the following session ids: %s", sessionIds));
            }

            // Commit to ensure that you leave the poll loop having reconnected successfully
            return BREAK;
        }

        return CONTINUE;
    }

    public Action onSlowStatusNotification(
        final int libraryId, final long connectionId, final boolean hasBecomeSlow)
    {
        if (libraryId == this.libraryId)
        {
            final SessionSubscriber subscriber = connectionIdToSession.get(connectionId);
            if (subscriber != null)
            {
                subscriber.onSlowStatusNotification(libraryId, hasBecomeSlow);
            }
        }

        return CONTINUE;
    }

    public Action onResetLibrarySequenceNumber(final int libraryId, final long sessionId)
    {
        if (libraryId == this.libraryId)
        {
            for (final SessionSubscriber subscriber : connectionIdToSession.values())
            {
                final Session session = subscriber.session();
                if (session.id() == sessionId)
                {
                    return Pressure.apply(session.resetSequenceNumbers());
                }
            }
        }

        return Action.CONTINUE;
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
            session, sessionIdStrategy, authenticationStrategy, validationStrategy, null);
        final SessionSubscriber subscriber = new SessionSubscriber(parser, session, receiveTimer, sessionTimer);
        connectionIdToSession.put(connectionId, subscriber);
        sessions = ArrayUtil.add(sessions, session);
    }

    private Session initiateSession(
        final long connectionId,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final SessionState state,
        final SessionConfiguration sessionConfiguration,
        final int sequenceIndex)
    {
        final int defaultInterval = configuration.defaultHeartbeatIntervalInS();
        final GatewayPublication publication = transport.outboundPublication();

        final int sessionBufferSize = configuration.sessionBufferSize();
        final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer(new byte[sessionBufferSize]);
        final SessionProxy sessionProxy = sessionProxy(connectionId, asciiBuffer);
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
            initiatorNewSequenceNumber(sessionConfiguration, lastSentSequenceNumber),
            sequenceIndex,
            state,
            sessionConfiguration != null && sessionConfiguration.resetSeqNum(),
            configuration.reasonableTransmissionTimeInMs(),
            asciiBuffer)
            .lastReceivedMsgSeqNum(
                initiatorNewSequenceNumber(sessionConfiguration, lastReceivedSequenceNumber) - 1);

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

    private int initiatorNewSequenceNumber(
        final SessionConfiguration sessionConfiguration, final int lastSequenceNumber)
    {
        final int newSequenceNumber = lastSequenceNumber + 1;
        if (sessionConfiguration == null)
        {
            return newSequenceNumber;
        }

        if (sessionConfiguration.hasCustomInitialSequenceNumber())
        {
            return sessionConfiguration.initialSequenceNumber();
        }

        if (sessionConfiguration.sequenceNumbersPersistent() && lastSequenceNumber != SessionInfo.UNK_SESSION)
        {
            return newSequenceNumber;
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
        final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer(new byte[sessionBufferSize]);

        return new AcceptorSession(
            heartbeatIntervalInS,
            connectionId,
            clock,
            sessionProxy(connectionId, asciiBuffer),
            publication,
            sessionIdStrategy,
            sendingTimeWindow,
            receivedMsgSeqNo,
            sentMsgSeqNo,
            libraryId,
            1,
            sequenceIndex,
            state,
            configuration.reasonableTransmissionTimeInMs(),
            asciiBuffer)
            .address(host, port);
    }

    private SessionProxy sessionProxy(final long connectionId, final MutableAsciiBuffer asciiBuffer)
    {
        return new SessionProxy(
            asciiBuffer,
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
        if (state == CLOSED)
        {
            throw new IllegalStateException("Library has been closed");
        }
    }

    private void closeWithParent()
    {
        try
        {
            fixLibrary.internalClose();
        }
        finally
        {
            close();
        }
    }

    public void close()
    {
        if (state != CLOSED)
        {
            connectionIdToSession.values().forEach(subscriber -> accessor.disable(subscriber.session()));
            state = CLOSED;
        }
    }
}
