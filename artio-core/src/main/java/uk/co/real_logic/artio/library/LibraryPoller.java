/*
 * Copyright 2015-2019 Real Logic Ltd, Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.library;

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
import uk.co.real_logic.artio.*;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.messages.ControlNotificationDecoder.SessionsDecoder;
import uk.co.real_logic.artio.protocol.*;
import uk.co.real_logic.artio.session.*;
import uk.co.real_logic.artio.timing.LibraryTimers;
import uk.co.real_logic.artio.timing.Timer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.ToIntFunction;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static java.util.Objects.requireNonNull;
import static uk.co.real_logic.artio.GatewayProcess.NO_CONNECTION_ID;
import static uk.co.real_logic.artio.GatewayProcess.NO_CORRELATION_ID;
import static uk.co.real_logic.artio.LogTag.*;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.artio.messages.SessionState.ACTIVE;

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

    /**
     * End of day operation has started
     */
    private static final int END_OF_DAY = 5;

    private final Long2ObjectHashMap<SessionSubscriber> connectionIdToSession = new Long2ObjectHashMap<>();
    private InternalSession[] sessions = new InternalSession[0];
    private InternalSession[] pendingInitiatorSessions = new InternalSession[0];

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

    // State changed during end of day operation
    private int sessionLogoutIndex = 0;

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

    boolean isAtEndOfDay()
    {
        return state == END_OF_DAY;
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
        validateEndOfDay();

        return new InitiateSessionReply(this, timeInMs() + configuration.timeoutInMs(), configuration);
    }

    Reply<SessionReplyStatus> releaseToGateway(final Session session, final long timeoutInMs)
    {
        requireNonNull(session, "session");
        validateEndOfDay();

        return new ReleaseToGatewayReply(
            this, timeInMs() + timeoutInMs, (InternalSession)session);
    }

    Reply<SessionReplyStatus> requestSession(
        final long sessionId,
        final int resendFromSequenceNumber,
        final int resendFromSequenceIndex,
        final long timeoutInMs)
    {
        validateEndOfDay();

        return new RequestSessionReply(
            this,
            timeInMs() + timeoutInMs,
            sessionId,
            resendFromSequenceNumber,
            resendFromSequenceIndex);
    }

    SessionWriter followerSession(final long id, final long connectionId, final int sequenceIndex)
    {
        checkState();

        return new SessionWriter(
            libraryId,
            id,
            connectionId,
            sessionBuffer(),
            outboundPublication,
            sequenceIndex);
    }

    Reply<SessionWriter> followerSession(final SessionHeaderEncoder headerEncoder, final long timeoutInMs)
    {
        validateEndOfDay();

        return new FollowerSessionReply(
            this, timeInMs() + timeoutInMs, headerEncoder);
    }

    void disableSession(final InternalSession session)
    {
        sessions = ArrayUtil.remove(sessions, session);
        session.disable();
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
            session.awaitingResend(),
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
            configuration.initialReceivedSequenceNumber(),
            configuration.initialSentSequenceNumber(),
            configuration.closedResendInterval(),
            configuration.resendRequestChunkSize(),
            configuration.sendRedundantResendRequests(),
            configuration.enableLastMsgSeqNumProcessed(),
            configuration.username(),
            configuration.password(),
            configuration.fixDictionary(),
            this.configuration.defaultHeartbeatIntervalInS(),
            correlationId);
    }

    long saveMidConnectionDisconnect(final long correlationId)
    {
        checkState();

        return outboundPublication.saveMidConnectionDisconnect(libraryId, correlationId);
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

    long saveFollowerSessionRequest(
        final long correlationId, final MutableAsciiBuffer buffer, final int offset, final int length)
    {
        checkState();

        return outboundPublication.saveFollowerSessionRequest(
            libraryId,
            correlationId,
            buffer,
            offset,
            length);
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

            case END_OF_DAY:
                attemptEndOfDayOperation();
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
        operations += pollPendingInitiatorSessions(timeInMs);
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
        // TODO: ban connecting from everything but library connect
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
        final InternalSession[] sessions = this.sessions;
        for (int i = 0, size = sessions.length; i < size; i++)
        {
            final InternalSession session = sessions[i];
            session.libraryConnected(libraryConnected);
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
        final InternalSession[] sessions = this.sessions;
        int total = 0;

        for (int i = 0, size = sessions.length; i < size; i++)
        {
            final InternalSession session = sessions[i];
            total += session.poll(timeInMs);
        }

        return total;
    }

    private int pollPendingInitiatorSessions(final long timeInMs)
    {
        InternalSession[] pendingSessions = this.pendingInitiatorSessions;
        int total = 0;

        for (int i = 0, size = pendingSessions.length; i < size;)
        {
            final InternalSession session = pendingSessions[i];
            total += session.poll(timeInMs);
            if (session.state() == ACTIVE)
            {
                this.pendingInitiatorSessions = pendingSessions = ArrayUtil.remove(pendingSessions, i);
                size--;
                sessions = ArrayUtil.add(sessions, session);
            }
            else
            {
                i++;
            }
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

    private final ControlledFragmentHandler outboundSubscription = new ControlledFragmentAssembler(
        ProtocolSubscription.of(this, new LibraryProtocolSubscription(this)));

    public Action onManageSession(
        final int libraryId,
        final long connection,
        final long sessionId,
        final int lastSentSeqNum,
        final int lastRecvSeqNum,
        final long logonTime,
        final SessionStatus sessionStatus,
        final SlowStatus slowStatus,
        final ConnectionType connectionType,
        final SessionState sessionState,
        final int heartbeatIntervalInS,
        final boolean closedResendInterval,
        final int resendRequestChunkSize,
        final boolean sendRedundantResendRequests,
        final boolean enableLastMsgSeqNumProcessed,
        final long correlationId,
        final int sequenceIndex,
        final boolean awaitingResend,
        final int lastResentMsgSeqNo,
        final int lastResendChunkMsgSeqNum,
        final int endOfResendRequestRange,
        final boolean awaitingHeartbeat,
        final String localCompId,
        final String localSubId,
        final String localLocationId,
        final String remoteCompId,
        final String remoteSubId,
        final String remoteLocationId,
        final String address,
        final String username,
        final String password,
        final Class<? extends FixDictionary> fixDictionaryType)
    {
        if (state == CONNECTED)
        {
            final FixDictionary fixDictionary = FixDictionary.of(fixDictionaryType);
            if (libraryId == ENGINE_LIBRARY_ID)
            {
                // Simple case of engine notifying that it has a session available.
                sessionExistsHandler.onSessionExists(
                    fixLibrary,
                    sessionId,
                    localCompId,
                    localSubId,
                    localLocationId,
                    remoteCompId,
                    remoteSubId,
                    remoteLocationId);
            }
            else if (libraryId == this.libraryId)
            {
                onHandoverSession(
                    libraryId,
                    connection,
                    sessionId,
                    lastSentSeqNum,
                    lastRecvSeqNum,
                    logonTime,
                    sessionStatus,
                    slowStatus,
                    connectionType,
                    sessionState,
                    heartbeatIntervalInS,
                    closedResendInterval,
                    resendRequestChunkSize,
                    sendRedundantResendRequests,
                    enableLastMsgSeqNumProcessed,
                    correlationId,
                    sequenceIndex,
                    awaitingResend,
                    lastResentMsgSeqNo,
                    lastResendChunkMsgSeqNum,
                    endOfResendRequestRange,
                    awaitingHeartbeat,
                    localCompId,
                    localSubId,
                    localLocationId,
                    remoteCompId,
                    remoteSubId,
                    remoteLocationId,
                    address,
                    username,
                    password,
                    fixDictionary);
            }
        }

        return CONTINUE;
    }

    private void onHandoverSession(
        final int libraryId,
        final long connection,
        final long sessionId,
        final int lastSentSeqNum,
        final int lastRecvSeqNum,
        final long logonTime,
        final SessionStatus sessionStatus,
        final SlowStatus slowStatus,
        final ConnectionType connectionType,
        final SessionState sessionState,
        final int heartbeatIntervalInS,
        final boolean closedResendInterval,
        final int resendRequestChunkSize,
        final boolean sendRedundantResendRequests,
        final boolean enableLastMsgSeqNumProcessed,
        final long correlationId,
        final int sequenceIndex,
        final boolean awaitingResend,
        final int lastResentMsgSeqNo,
        final int lastResendChunkMsgSeqNum,
        final int endOfResendRequestRange,
        final boolean awaitingHeartbeat,
        final String localCompId,
        final String localSubId,
        final String localLocationId,
        final String remoteCompId,
        final String remoteSubId,
        final String remoteLocationId,
        final String address,
        final String username,
        final String password,
        final FixDictionary fixDictionary)
    {
        final InternalSession session;
        InitiateSessionReply reply = null;
        if (sessionStatus == SessionStatus.SESSION_HANDOVER)
        {
            // From manageConnection - ie set up the session in this library.
            if (connectionType == INITIATOR)
            {
                DebugLogger.log(FIX_CONNECTION, "Init Connect: %d, %d%n", connection, libraryId);
                final boolean isReply = correlationIdToReply.get(correlationId) instanceof InitiateSessionReply;
                if (isReply)
                {
                    reply = (InitiateSessionReply)correlationIdToReply.remove(correlationId);
                }
                session = newInitiatorSession(
                    connection,
                    lastSentSeqNum,
                    lastRecvSeqNum,
                    sessionState,
                    isReply ? reply.configuration() : null,
                    sequenceIndex,
                    enableLastMsgSeqNumProcessed,
                    fixDictionary);
            }
            else
            {
                DebugLogger.log(FIX_CONNECTION, "Acct Connect: %d, %d%n", connection, libraryId);
                session = acceptSession(
                    connection, address, sessionState, heartbeatIntervalInS, sequenceIndex,
                    enableLastMsgSeqNumProcessed, fixDictionary);
                session.lastSentMsgSeqNum(lastSentSeqNum);
                session.initialLastReceivedMsgSeqNum(lastRecvSeqNum);
            }

            final CompositeKey compositeKey = sessionIdStrategy.onInitiateLogon(
                localCompId,
                localSubId,
                localLocationId,
                remoteCompId,
                remoteSubId,
                remoteLocationId);

            session.username(username);
            session.password(password);
            session.setupSession(sessionId, compositeKey);
            session.logonTime(logonTime);
            session.closedResendInterval(closedResendInterval);
            session.resendRequestChunkSize(resendRequestChunkSize);
            session.sendRedundantResendRequests(sendRedundantResendRequests);
            session.awaitingResend(awaitingResend);
            session.lastResentMsgSeqNo(lastResentMsgSeqNo);
            session.lastResendChunkMsgSeqNum(lastResendChunkMsgSeqNum);
            session.endOfResendRequestRange(endOfResendRequestRange);
            session.awaitingHeartbeat(awaitingHeartbeat);

            createSessionSubscriber(connection, session, reply, slowStatus, fixDictionary);
            insertSession(session, connectionType, sessionState);

            DebugLogger.log(GATEWAY_MESSAGE,
                "onSessionExists: conn=%d, sess=%d, sentSeqNo=%d, recvSeqNo=%d%n",
                connection,
                sessionId,
                lastSentSeqNum,
                lastRecvSeqNum);
        }
        else
        {
            sessionExistsHandler.onSessionExists(
                fixLibrary,
                sessionId,
                localCompId,
                localSubId,
                localLocationId,
                remoteCompId,
                remoteSubId,
                remoteLocationId);
        }
    }

    private void insertSession(
        final InternalSession session, final ConnectionType connectionType, final SessionState sessionState)
    {
        if (connectionType == INITIATOR && sessionState != ACTIVE)
        {
            pendingInitiatorSessions = ArrayUtil.add(pendingInitiatorSessions, session);
        }
        else
        {
            sessions = ArrayUtil.add(sessions, session);
        }
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
        final int sequenceNumber,
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
                    final InternalSession session = subscriber.session();
                    session.close();
                    // session will be in either pendingInitiatorSessions or sessions
                    pendingInitiatorSessions = ArrayUtil.remove(pendingInitiatorSessions, session);
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
                reply.onError(errorType, message);
            }
            else
            {
                // case of a connect error
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
        final ReleaseToGatewayReply reply = (ReleaseToGatewayReply)correlationIdToReply.remove(replyToId);
        if (reply != null)
        {
            reply.onComplete(status);
        }

        return CONTINUE;
    }

    public Action onRequestSessionReply(final int libraryId, final long replyToId, final SessionReplyStatus status)
    {
        final RequestSessionReply reply = (RequestSessionReply)correlationIdToReply.remove(replyToId);
        if (reply != null)
        {
            reply.onComplete(status);
        }

        return CONTINUE;
    }

    public Action onFollowerSessionReply(final int libraryId, final long replyToId, final long sessionId)
    {
        final FollowerSessionReply reply = (FollowerSessionReply)correlationIdToReply.remove(replyToId);
        if (reply != null)
        {
            reply.onComplete(followerSession(sessionId, NO_CONNECTION_ID, 0));
        }

        return CONTINUE;
    }

    public Action onEndOfDay(final int libraryId)
    {
        if (libraryId == this.libraryId)
        {
            state = END_OF_DAY;

            attemptEndOfDayOperation();
        }

        return CONTINUE;
    }

    private void attemptEndOfDayOperation()
    {
        final InternalSession[] sessions = this.sessions;
        final int length = sessions.length;

        while (sessionLogoutIndex < length)
        {
            final long position = sessions[sessionLogoutIndex].logoutAndDisconnect();
            if (position < 0)
            {
                return;
            }

            sessionLogoutIndex++;
        }

        state = CLOSED;
    }

    private void validateEndOfDay()
    {
        if (isAtEndOfDay())
        {
            throw new IllegalStateException("Cannot perform operation whilst end of day process is running");
        }
    }

    public Action onNewSentPosition(final int libraryId, final long position)
    {
        if (this.libraryId == libraryId)
        {
            return sentPositionHandler.onSendCompleted(position);
        }

        return CONTINUE;
    }

    public Action onControlNotification(final int libraryId, final SessionsDecoder sessionsDecoder)
    {
        // TODO(Nick): Reorganise this as it is confusing.
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

            // TODO(Nick): This should be a new set, not the actual
            // set as we remove all the ids to check for existence..
            // Weirdly looks like this.sessionIds is never used anywhere else?
            // Why do we have it then? Is the caching saving
            // Is the caching saving enough considering that below we are creating loads of arrays (potentially)
            final LongHashSet sessionIds = this.sessionIds;
            InternalSession[] sessions = this.sessions;

            // copy session ids.
            sessionIds.clear();
            while (sessionsDecoder.hasNext())
            {
                sessionsDecoder.next();
                sessionIds.add(sessionsDecoder.sessionId());
            }

            for (int i = 0, size = sessions.length; i < size; i++)
            {
                final InternalSession session = sessions[i];
                final long sessionId = session.id();
                if (!sessionIds.remove(sessionId))
                {
                    final SessionSubscriber subscriber = connectionIdToSession.remove(session.connectionId());
                    if (subscriber != null)
                    {
                        subscriber.onTimeout(libraryId);
                    }
                    session.close();
                    // TODO(Nick): Maybe we shouldn't be creating a lot of arrays and batch this up?
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
                final String msg = String.format(
                    "The gateway thinks that we own the following session ids: %s", sessionIds);
                configuration
                    .gatewayErrorHandler()
                    .onError(GatewayError.UNKNOWN_SESSION, libraryId, msg);
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

    private void createSessionSubscriber(
        final long connectionId,
        final InternalSession session,
        final InitiateSessionReply reply,
        final SlowStatus slowStatus,
        final FixDictionary fixDictionary)
    {
        final MessageValidationStrategy validationStrategy = configuration.messageValidationStrategy();
        final SessionParser parser = new SessionParser(
            session, validationStrategy, null, fixDictionary);
        final SessionSubscriber subscriber = new SessionSubscriber(
            parser,
            session,
            receiveTimer,
            sessionTimer);
        subscriber.reply(reply);
        subscriber.handler(configuration.sessionAcquireHandler()
            .onSessionAcquired(session, SlowStatus.SLOW == slowStatus));

        connectionIdToSession.put(connectionId, subscriber);
    }

    private InitiatorSession newInitiatorSession(
        final long connectionId,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final SessionState state,
        final SessionConfiguration sessionConfiguration,
        final int sequenceIndex,
        final boolean enableLastMsgSeqNumProcessed,
        final FixDictionary fixDictionary)
    {
        final int defaultInterval = configuration.defaultHeartbeatIntervalInS();
        final GatewayPublication publication = transport.outboundPublication();

        final MutableAsciiBuffer asciiBuffer = sessionBuffer();
        final SessionProxy sessionProxy = sessionProxy(connectionId, fixDictionary);
        final int initialReceivedSequenceNumber = initiatorNewSequenceNumber(
            sessionConfiguration, SessionConfiguration::initialReceivedSequenceNumber, lastReceivedSequenceNumber);
        final int initialSentSequenceNumber = initiatorNewSequenceNumber(
            sessionConfiguration, SessionConfiguration::initialSentSequenceNumber, lastSentSequenceNumber);

        final InitiatorSession session = new InitiatorSession(
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
            initialSentSequenceNumber,
            sequenceIndex,
            state,
            sessionConfiguration != null && sessionConfiguration.resetSeqNum(),
            configuration.reasonableTransmissionTimeInMs(),
            asciiBuffer,
            enableLastMsgSeqNumProcessed,
            fixDictionary.beginString());

        session.initialLastReceivedMsgSeqNum(initialReceivedSequenceNumber - 1);

        return session;
    }

    private MutableAsciiBuffer sessionBuffer()
    {
        return new MutableAsciiBuffer(new byte[configuration.sessionBufferSize()]);
    }

    private int initiatorNewSequenceNumber(
        final SessionConfiguration sessionConfiguration,
        final ToIntFunction<SessionConfiguration> initialSequenceNumberGetter,
        final int lastSequenceNumber)
    {
        final int newSequenceNumber = lastSequenceNumber + 1;
        if (sessionConfiguration == null)
        {
            return newSequenceNumber;
        }

        final int initialSequenceNumber = initialSequenceNumberGetter.applyAsInt(sessionConfiguration);
        if (initialSequenceNumber != AUTOMATIC_INITIAL_SEQUENCE_NUMBER)
        {
            return initialSequenceNumber;
        }

        if (sessionConfiguration.sequenceNumbersPersistent() && lastSequenceNumber != SessionInfo.UNK_SESSION)
        {
            return newSequenceNumber;
        }

        return 1;
    }

    private InternalSession acceptSession(
        final long connectionId,
        final String address,
        final SessionState state,
        final int heartbeatIntervalInS,
        final int sequenceIndex,
        final boolean enableLastMsgSeqNumProcessed,
        final FixDictionary fixDictionary)
    {
        final GatewayPublication publication = transport.outboundPublication();
        final long sendingTimeWindow = configuration.sendingTimeWindowInMs();
        final AtomicCounter receivedMsgSeqNo = fixCounters.receivedMsgSeqNo(connectionId);
        final AtomicCounter sentMsgSeqNo = fixCounters.sentMsgSeqNo(connectionId);
        final MutableAsciiBuffer asciiBuffer = sessionBuffer();
        final int split = address.lastIndexOf(':');
        final int start = address.startsWith("/") ? 1 : 0;
        final String host = address.substring(start, split);
        final int port = Integer.parseInt(address.substring(split + 1));

        final InternalSession session = new AcceptorSession(
            heartbeatIntervalInS,
            connectionId,
            clock,
            sessionProxy(connectionId, fixDictionary),
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
            asciiBuffer,
            enableLastMsgSeqNumProcessed,
            fixDictionary.beginString());
        session.address(host, port);
        return session;
    }

    private SessionProxy sessionProxy(final long connectionId, final FixDictionary fixDictionary)
    {
        return configuration.sessionProxyFactory().make(
            configuration.sessionBufferSize(),
            transport.outboundPublication(),
            sessionIdStrategy,
            configuration.sessionCustomisationStrategy(),
            new SystemEpochClock(),
            connectionId,
            libraryId,
            fixDictionary,
            LangUtil::rethrowUnchecked);
    }

    private void checkState()
    {
        if (state != CONNECTED)
        {
            throw new IllegalStateException("Library has been closed or is performing end of day operation");
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
        if (state != CLOSED && configuration.gracefulShutdown())
        {
            connectionIdToSession.values().forEach(subscriber -> subscriber.session().disable());
            state = CLOSED;
        }
    }

}
