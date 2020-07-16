/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.CollectionUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.*;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.ConnectedSessionInfo;
import uk.co.real_logic.artio.engine.RecordingCoordinator;
import uk.co.real_logic.artio.ilink.AbstractILink3Parser;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.messages.ControlNotificationDecoder.SessionsDecoder;
import uk.co.real_logic.artio.protocol.*;
import uk.co.real_logic.artio.session.*;
import uk.co.real_logic.artio.timing.LibraryTimers;
import uk.co.real_logic.artio.timing.Timer;
import uk.co.real_logic.artio.util.CharFormatter;
import uk.co.real_logic.artio.util.EpochFractionClock;
import uk.co.real_logic.artio.util.EpochFractionClocks;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static java.util.Objects.requireNonNull;
import static uk.co.real_logic.artio.GatewayProcess.NO_CONNECTION_ID;
import static uk.co.real_logic.artio.GatewayProcess.NO_CORRELATION_ID;
import static uk.co.real_logic.artio.LogTag.*;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.artio.messages.DisconnectReason.ENGINE_SHUTDOWN;
import static uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner.SOLE_LIBRARY;
import static uk.co.real_logic.artio.messages.SessionState.ACTIVE;
import static uk.co.real_logic.artio.session.Session.UNKNOWN_TIME;

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
     * Engine is disconnected by either closing or unbind operation
     */
    private static final int ENGINE_DISCONNECT = 5;

    private static final ILink3Connection[] EMPTY_ILINK_CONNECTIONS = new ILink3Connection[0];
    private static final InternalSession[] EMPTY_SESSIONS = new InternalSession[0];

    private final Long2ObjectHashMap<WeakReference<InternalSession>> sessionIdToCachedSession =
        new Long2ObjectHashMap<>();
    private final Long2ObjectHashMap<SessionSubscriber> connectionIdToSession = new Long2ObjectHashMap<>();
    private ILink3Connection[] iLink3Connections = EMPTY_ILINK_CONNECTIONS;
    private final List<ILink3Connection> unmodifiableILink3Connections =
        new UnmodifiableWrapper<>(() -> iLink3Connections);

    private InternalSession[] sessions = EMPTY_SESSIONS;
    private InternalSession[] pendingInitiatorSessions = EMPTY_SESSIONS;
    private final List<Session> unmodifiableSessions = new UnmodifiableWrapper<>(() -> sessions);

    private final Long2ObjectHashMap<ILink3Subscription> connectionIdToILink3Subscription = new Long2ObjectHashMap<>();

    private static final ErrorHandler THROW_ERRORS = LangUtil::rethrowUnchecked;

    // Used when checking the consistency of the session ids
    private final LongHashSet sessionIds = new LongHashSet();

    // Uniquely identifies library session
    private final int libraryId;
    private final EpochClock epochClock;
    private final EpochFractionClock epochFractionClock;
    private final LibraryConfiguration configuration;
    private final SessionIdStrategy sessionIdStrategy;
    private final Timer sessionTimer;
    private final Timer receiveTimer;
    private final SessionExistsHandler sessionExistsHandler;
    private final boolean enginesAreClustered;
    private final FixCounters fixCounters;

    private final Long2ObjectHashMap<LibraryReply<?>> correlationIdToReply = new Long2ObjectHashMap<>();
    private final List<BooleanSupplier> tasks = new ArrayList<>();
    private final LibraryTransport transport;
    private final FixLibrary fixLibrary;
    private final Runnable onDisconnectFunc = this::onDisconnect;
    private final SessionAcquiredInfo sessionAcquiredInfo = new SessionAcquiredInfo();

    private final CharFormatter receivedFormatter = new CharFormatter("(%s) Received %s %n");
    private final CharFormatter disconnectedFormatter = new CharFormatter("%s: Disconnected from [%s]%n");
    private final CharFormatter connectedFormatter = new CharFormatter("%s: Connected to [%s]%n");
    private final CharFormatter attemptConnectFormatter = new CharFormatter("%s: Attempting to connect to %s%n");
    private final CharFormatter attemptNextFormatter = new CharFormatter(
        "%s: Attempting connect to next engine (%s) in round-robin%n");
    private final CharFormatter initiatorConnectFormatter = new CharFormatter("Init Connect: %s, %s%n");
    private final CharFormatter acceptorConnectFormatter = new CharFormatter("Acct Connect: %s, %s%n");
    private final CharFormatter controlNotificationFormatter = new CharFormatter(
        "%s: Received Control Notification from engine at timeInMs %s%n");
    private final CharFormatter applicationHeartbeatFormatter = new CharFormatter(
        "%s: Received Heartbeat from engine at timeInMs %s%n");
    private final CharFormatter reconnectFormatter = new CharFormatter("Reconnect: %s, %s, %s%n");
    private final CharFormatter onDisconnectFormatter = new CharFormatter(
        "%s: Session Disconnect @ Library %s, %s%n");
    private final CharFormatter sessionExistsFormatter = new CharFormatter(
        "onSessionExists: conn=%s, sess=%s, sentSeqNo=%s, recvSeqNo=%s%n");

    /**
     * Correlation Id is initialised to a random number to reduce the chance of correlation id collision.
     */
    private long currentCorrelationId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);

    private InitialAcceptedSessionOwner initialAcceptedSessionOwner;
    private int state = CONNECTING;

    // State changed upon connect/reconnect
    private LivenessDetector livenessDetector;
    private Subscription inboundSubscription;
    private GatewayPublication inboundPublication;
    private GatewayPublication outboundPublication;
    private String currentAeronChannel;
    private long nextSendLibraryConnectTime;
    private long nextEngineAttemptTime;

    // Combined with Library Id, uniquely identifies library connection
    private long connectCorrelationId = NO_CORRELATION_ID;

    // State changed during end of day operation
    private int sessionLogoutIndex = 0;
    private Iterator<ILink3Subscription> iLink3LogoutIterator = null;

    LibraryPoller(
        final LibraryConfiguration configuration,
        final LibraryTimers timers,
        final FixCounters fixCounters,
        final LibraryTransport transport,
        final FixLibrary fixLibrary,
        final EpochClock epochClock)
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
        this.epochClock = epochClock;
        this.enginesAreClustered = configuration.libraryAeronChannels().size() > 1;
        this.epochFractionClock = EpochFractionClocks.create(
            epochClock, configuration.epochNanoClock(), configuration.sessionEpochFractionFormat());
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
        return state == ENGINE_DISCONNECT;
    }

    int libraryId()
    {
        return libraryId;
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

    public Reply<ILink3Connection> initiate(final ILink3ConnectionConfiguration configuration)
    {
        requireNonNull(configuration, "configuration");
        validateEndOfDay();

        return new InitiateILink3ConnectionReply(
            this, timeInMs() + configuration.requestedKeepAliveIntervalInMs(), configuration);
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

    Reply<MetaDataStatus> writeMetaData(
        final long sessionId, final int metaDataOffset, final DirectBuffer buffer, final int offset, final int length)
    {
        if (metaDataOffset < 0)
        {
            throw new IllegalArgumentException("metaDataOffset should never be negative and is " + metaDataOffset);
        }

        return new WriteMetaDataReply(
            this,
            timeInMs() + configuration.replyTimeoutInMs(),
            sessionId,
            metaDataOffset,
            buffer,
            offset,
            length);
    }

    public void readMetaData(final long sessionId, final MetadataHandler handler)
    {
        new ReadMetaDataReply(
            this, timeInMs() + configuration.replyTimeoutInMs(), sessionId, handler);
    }

    void disableSession(final InternalSession session)
    {
        sessions = ArrayUtil.remove(sessions, session);
        session.disable();
        cacheSession(session);
    }

    private void cacheSession(final InternalSession session)
    {
        sessionIdToCachedSession.put(session.id(), new WeakReference<>(session));
    }

    long saveWriteMetaData(
        final long sessionId,
        final int metaDataOffset,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long correlationId)
    {
        checkState();

        return outboundPublication.saveWriteMetaData(
            libraryId,
            sessionId,
            metaDataOffset,
            correlationId,
            buffer,
            offset,
            length);
    }

    long saveReadMetaData(final long sessionId, final long correlationId)
    {
        checkState();

        return outboundPublication.saveReadMetaData(
            libraryId,
            sessionId,
            correlationId);
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

    long saveReplayMessages(
        final long sessionId,
        final long correlationId,
        final int replayFromSequenceNumber,
        final int replayFromSequenceIndex,
        final int replayToSequenceNumber,
        final int replayToSequenceIndex,
        final long latestReplyArrivalTimeInMs)
    {
        checkState();

        return outboundPublication.saveReplayMessages(
            libraryId,
            sessionId,
            correlationId,
            replayFromSequenceNumber,
            replayFromSequenceIndex,
            replayToSequenceNumber,
            replayToSequenceIndex,
            latestReplyArrivalTimeInMs);
    }

    void onInitiatorSessionTimeout(final long correlationId, final long connectionId)
    {
        checkState();

        if (connectionId == NO_CONNECTION_ID)
        {
            onTimeoutWaitingForConnection(correlationId);
        }
        else
        {
            // We've timed out when the TCP connection has completed, but the logon hasn't finished.

            if (!saveNoLogonRequestDisconnect(connectionId))
            {
                tasks.add(() -> saveNoLogonRequestDisconnect(connectionId));
            }
        }
    }

    void onTimeoutWaitingForConnection(final long correlationId)
    {
        if (!saveMidConnectionDisconnect(correlationId))
        {
            tasks.add(() -> saveMidConnectionDisconnect(correlationId));
        }
    }

    private boolean saveMidConnectionDisconnect(final long correlationId)
    {
        return outboundPublication.saveMidConnectionDisconnect(libraryId, correlationId) > 0;
    }

    private boolean saveNoLogonRequestDisconnect(final long connectionId)
    {
        return outboundPublication.saveRequestDisconnect(libraryId, connectionId, DisconnectReason.NO_LOGON) > 0;
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

            case ENGINE_DISCONNECT:
                attemptEngineCloseBasedLogout();
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
                attemptConnectFormatter,
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
                attemptNextFormatter,
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
            inboundPublication = transport.inboundPublication();
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
            connectedFormatter,
            libraryId,
            currentAeronChannel);
        configuration.libraryConnectHandler().onConnect(fixLibrary);
        setLibraryConnected(true);
    }

    private void onDisconnect()
    {
        DebugLogger.log(
            LIBRARY_CONNECT,
            disconnectedFormatter,
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

        final ILink3Connection[] iLink3Connections = this.iLink3Connections;
        for (int i = 0, size = iLink3Connections.length; i < size; i++)
        {
            final ILink3Connection connection = iLink3Connections[i];
            total += connection.poll(timeInMs);
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

    long timeInMs()
    {
        return epochClock.time();
    }

    private int checkReplies(final long timeInMs)
    {
        int count = 0;
        final Long2ObjectHashMap<LibraryReply<?>>.ValueIterator iterator = correlationIdToReply.values().iterator();
        while (iterator.hasNext())
        {
            final LibraryReply<?> reply = iterator.next();
            if (reply.poll(timeInMs))
            {
                iterator.remove();
                count++;
            }
        }

        CollectionUtil.removeIf(tasks, BooleanSupplier::getAsBoolean);

        return count;
    }

    long register(final LibraryReply<?> reply)
    {
        final long correlationId = ++currentCorrelationId;
        correlationIdToReply.put(correlationId, reply);
        return correlationId;
    }

    void deregister(final long correlationId)
    {
        correlationIdToReply.remove(correlationId);
    }

    // -----------------------------------------------------------------------
    //                     BEGIN EVENT HANDLERS
    // -----------------------------------------------------------------------

    private final ControlledFragmentHandler outboundSubscription = new ControlledFragmentAssembler(
        ProtocolSubscription.of(this, new LibraryProtocolSubscription(this)));

    public Action onManageSession(
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int lastSentSeqNum,
        final int lastRecvSeqNum,
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
        final int logonReceivedSequenceNumber,
        final int logonSequenceIndex,
        final long lastLogonTime,
        final long lastSequenceResetTime,
        final String localCompId,
        final String localSubId,
        final String localLocationId,
        final String remoteCompId,
        final String remoteSubId,
        final String remoteLocationId,
        final String address,
        final String username,
        final String password,
        final Class<? extends FixDictionary> fixDictionaryType,
        final MetaDataStatus metaDataStatus,
        final DirectBuffer metaDataBuffer,
        final int metaDataOffset,
        final int metaDataLength)
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
                    remoteLocationId,
                    logonReceivedSequenceNumber,
                    logonSequenceIndex);
            }
            else if (libraryId == this.libraryId)
            {
                if (sessionStatus == SessionStatus.SESSION_HANDOVER)
                {
                    sessionAcquiredInfo.wrap(
                        slowStatus, metaDataStatus, metaDataBuffer, metaDataOffset, metaDataLength);
                    onHandoverSession(
                        libraryId,
                        connectionId,
                        sessionId,
                        lastSentSeqNum, lastRecvSeqNum,
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
                        lastLogonTime, lastSequenceResetTime,
                        localCompId, localSubId, localLocationId,
                        remoteCompId, remoteSubId, remoteLocationId,
                        address,
                        username,
                        password,
                        fixDictionary);
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
                        remoteLocationId,
                        logonReceivedSequenceNumber,
                        logonSequenceIndex);
                }
            }
        }

        return CONTINUE;
    }

    private void onHandoverSession(
        final int libraryId, final long connectionId, final long sessionId,
        final int lastSentSeqNum, final int lastRecvSeqNum,
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
        final long lastLogonTime,
        final long lastSequenceResetTime,
        final String localCompId, final String localSubId, final String localLocationId,
        final String remoteCompId, final String remoteSubId, final String remoteLocationId,
        final String address,
        final String username, final String password,
        final FixDictionary fixDictionary)
    {
        InternalSession session;
        InitiateSessionReply reply = null;
        session = checkReconnect(sessionId, connectionId, sessionState, heartbeatIntervalInS,
            sequenceIndex, enableLastMsgSeqNumProcessed, fixDictionary, connectionType, address);
        final boolean isNewConnect = session == null;

        final OnMessageInfo messageInfo = isNewConnect ? new OnMessageInfo() : session.messageInfo();

        // From manageConnection - ie set up the session in this library.
        if (connectionType == INITIATOR)
        {
            DebugLogger.log(FIX_CONNECTION, initiatorConnectFormatter, connectionId, libraryId);
            final LibraryReply<?> task = correlationIdToReply.get(correlationId);
            final boolean isReply = task instanceof InitiateSessionReply;
            if (isReply)
            {
                reply = (InitiateSessionReply)task;
                reply.onTcpConnected(connectionId);
            }
            final SessionConfiguration sessionConfiguration = isReply ? reply.configuration() : null;
            final int initialReceivedSequenceNumber = initiatorNewSequenceNumber(
                sessionConfiguration, SessionConfiguration::initialReceivedSequenceNumber, lastRecvSeqNum);
            final int initialSentSequenceNumber = initiatorNewSequenceNumber(
                sessionConfiguration, SessionConfiguration::initialSentSequenceNumber, lastSentSeqNum);
            final boolean resetSeqNum = sessionConfiguration != null && sessionConfiguration.resetSeqNum();

            if (isNewConnect)
            {
                session = newInitiatorSession(
                    connectionId,
                    initialSentSequenceNumber, initialReceivedSequenceNumber,
                    sessionState,
                    sequenceIndex,
                    enableLastMsgSeqNumProcessed,
                    fixDictionary,
                    resetSeqNum,
                    messageInfo);
            }
            else
            {
                session.lastSentMsgSeqNum(initialSentSequenceNumber - 1);
                session.lastReceivedMsgSeqNumOnly(initialReceivedSequenceNumber - 1);
            }
        }
        else
        {
            DebugLogger.log(FIX_CONNECTION, acceptorConnectFormatter, connectionId, libraryId);
            if (isNewConnect)
            {
                session = acceptSession(
                    connectionId, address, sessionState, heartbeatIntervalInS, sequenceIndex,
                    enableLastMsgSeqNumProcessed, fixDictionary, messageInfo);
                session.initialLastReceivedMsgSeqNum(lastRecvSeqNum);
            }
            else
            {
                session.lastReceivedMsgSeqNumOnly(lastRecvSeqNum);
            }
            session.lastSentMsgSeqNum(lastSentSeqNum);
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
        session.closedResendInterval(closedResendInterval);
        session.resendRequestChunkSize(resendRequestChunkSize);
        session.sendRedundantResendRequests(sendRedundantResendRequests);
        session.awaitingResend(awaitingResend);
        session.lastResentMsgSeqNo(lastResentMsgSeqNo);
        session.lastResendChunkMsgSeqNum(lastResendChunkMsgSeqNum);
        session.endOfResendRequestRange(endOfResendRequestRange);
        session.awaitingHeartbeat(awaitingHeartbeat);
        if (lastLogonTime != UNKNOWN_TIME)
        {
            session.lastLogonTime(lastLogonTime);
        }
        if (lastSequenceResetTime != UNKNOWN_TIME)
        {
            session.lastSequenceResetTime(lastSequenceResetTime);
        }

        createSessionSubscriber(connectionId, session, reply, fixDictionary, messageInfo, compositeKey);
        if (isNewConnect)
        {
            insertSession(session, connectionType, sessionState);
        }

        DebugLogger.log(GATEWAY_MESSAGE,
            sessionExistsFormatter,
            connectionId, sessionId, lastSentSeqNum, lastRecvSeqNum);
    }

    // Either a reconnect of an offline session, or the cache.
    private InternalSession checkReconnect(
        final long sessionId,
        final long connectionId,
        final SessionState sessionState,
        final int heartbeatIntervalInS,
        final int sequenceIndex,
        final boolean enableLastMsgSeqNumProcessed,
        final FixDictionary fixDictionary,
        final ConnectionType connectionType,
        final String address)
    {
        final InternalSession[] sessions = this.sessions;
        for (final InternalSession session : sessions)
        {
            if (session.id() == sessionId)
            {
                DebugLogger.log(FIX_CONNECTION, reconnectFormatter, connectionId, libraryId, sessionId);

                session.onReconnect(
                    connectionId,
                    sessionState,
                    heartbeatIntervalInS,
                    sequenceIndex,
                    enableLastMsgSeqNumProcessed,
                    fixDictionary,
                    address,
                    fixCounters);
                return session;
            }
        }

        final WeakReference<InternalSession> reference = sessionIdToCachedSession.remove(sessionId);
        if (reference != null)
        {
            final InternalSession session = reference.get();
            if (session != null)
            {
                insertSession(session, connectionType, sessionState);
                session.onReconnect(
                    connectionId,
                    sessionState,
                    heartbeatIntervalInS,
                    sequenceIndex,
                    enableLastMsgSeqNumProcessed,
                    fixDictionary,
                    address,
                    fixCounters);
            }
            return session;
        }
        return null;
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
        final long messageType,
        final long timestamp,
        final MessageStatus status,
        final int sequenceNumber,
        final long position,
        final int metaDataLength)
    {
        if (libraryId == this.libraryId)
        {
            DebugLogger.log(FIX_MESSAGE, receivedFormatter, libraryId, buffer, offset, length);

            final SessionSubscriber subscriber = connectionIdToSession.get(connectionId);
            if (subscriber != null)
            {
                return subscriber.onMessage(
                    buffer,
                    offset,
                    length,
                    libraryId,
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
        DebugLogger.log(GATEWAY_MESSAGE, onDisconnectFormatter, libraryId, connectionId, reason.name());
        if (libraryId != this.libraryId)
        {
            return CONTINUE;
        }

        final boolean soleLibraryMode = initialAcceptedSessionOwner == SOLE_LIBRARY;
        if (soleLibraryMode)
        {
            final SessionSubscriber subscriber = connectionIdToSession.get(connectionId);
            if (subscriber != null)
            {
                return subscriber.onDisconnect(libraryId, reason);
            }
            else
            {
                // iLink3 doesn't behave any differently in sole library mode as it's an initiator connection.
                onILink3Disconnect(connectionId);
            }
        }
        else
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
                    cacheSession(session);
                }

                return action;
            }
            else
            {
                onILink3Disconnect(connectionId);
            }
        }

        return CONTINUE;
    }

    private void onILink3Disconnect(final long connectionId)
    {
        final ILink3Subscription subscription = connectionIdToILink3Subscription.remove(connectionId);
        if (subscription != null)
        {
            subscription.onDisconnect();
            remove(subscription.session());
        }
    }

    public Action onILinkMessage(final long connectionId, final DirectBuffer buffer, final int offset)
    {
        final ILink3Subscription subscription = connectionIdToILink3Subscription.get(connectionId);
        if (subscription != null)
        {
            return Pressure.apply(subscription.onMessage(buffer, offset));
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
                APPLICATION_HEARTBEAT, applicationHeartbeatFormatter, libraryId, timeInMs);
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

    public Action onReplayMessagesReply(final int libraryId, final long replyToId, final ReplayMessagesStatus status)
    {
        final ReplayMessagesReply reply = (ReplayMessagesReply)correlationIdToReply.remove(replyToId);
        if (reply != null)
        {
            reply.onComplete(status);
        }

        return CONTINUE;
    }

    public Action onWriteMetaDataReply(final int libraryId, final long replyToId, final MetaDataStatus status)
    {
        final WriteMetaDataReply reply = (WriteMetaDataReply)correlationIdToReply.remove(replyToId);
        if (reply != null)
        {
            reply.onComplete(status);
        }

        return CONTINUE;
    }

    public Action onILinkConnect(
        final int libraryId,
        final long correlationId,
        final long connectionId,
        final long uuid,
        final long lastReceivedSequenceNumber,
        final long lastSentSequenceNumber,
        final boolean newlyAllocated,
        final long lastUuid)
    {
        if (libraryId == this.libraryId)
        {
            final InitiateILink3ConnectionReply reply =
                (InitiateILink3ConnectionReply)correlationIdToReply.remove(correlationId);

            if (reply != null)
            {
                DebugLogger.log(FIX_CONNECTION, initiatorConnectFormatter, connectionId, libraryId);

                reply.onTcpConnected();

                final ILink3ConnectionConfiguration configuration = reply.configuration();
                final ILink3Connection connection = makeILink3Connection(
                    configuration, connectionId, reply, libraryId, this,
                    uuid, lastReceivedSequenceNumber, lastSentSequenceNumber, newlyAllocated, lastUuid);
                final ILink3Subscription subscription = new ILink3Subscription(
                    AbstractILink3Parser.make(connection, THROW_ERRORS), connection);
                connectionIdToILink3Subscription.put(connectionId, subscription);
                iLink3Connections = ArrayUtil.add(iLink3Connections, connection);
            }
        }

        return CONTINUE;
    }

    private ILink3Connection makeILink3Connection(
        final ILink3ConnectionConfiguration configuration,
        final long connectionId,
        final InitiateILink3ConnectionReply initiateReply,
        final int libraryId,
        final LibraryPoller owner,
        final long uuid,
        final long lastReceivedSequenceNumber,
        final long lastSentSequenceNumber,
        final boolean newlyAllocated,
        final long lastUuid)
    {
        try
        {
            final Class<?> cls = Class.forName("uk.co.real_logic.artio.library.InternalILink3Connection");
            final Constructor<?> constructor = cls.getConstructor(
                ILink3ConnectionConfiguration.class,
                long.class,
                InitiateILink3ConnectionReply.class,
                GatewayPublication.class,
                GatewayPublication.class,
                int.class,
                LibraryPoller.class,
                long.class,
                long.class,
                long.class,
                boolean.class,
                long.class,
                EpochNanoClock.class);

            return (ILink3Connection)constructor.newInstance(
                configuration,
                connectionId,
                initiateReply,
                outboundPublication,
                inboundPublication,
                libraryId,
                owner,
                uuid,
                lastReceivedSequenceNumber,
                lastSentSequenceNumber,
                newlyAllocated,
                lastUuid,
                this.configuration.epochNanoClock());
        }
        catch (final InvocationTargetException e)
        {
            LangUtil.rethrowUnchecked(e.getTargetException());
            return null;
        }
        catch (final ClassNotFoundException | NoSuchMethodException | InstantiationException |
            IllegalAccessException e)
        {
            LangUtil.rethrowUnchecked(e);
            return null;
        }
    }

    public Action onReadMetaDataReply(
        final int libraryId,
        final long replyToId,
        final MetaDataStatus status,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength)
    {
        final ReadMetaDataReply reply = (ReadMetaDataReply)correlationIdToReply.remove(replyToId);
        if (reply != null)
        {
            reply.onComplete(status, srcBuffer, srcOffset, srcLength);
        }

        return CONTINUE;
    }

    public Action onEngineClose(final int libraryId)
    {
        if (libraryId == this.libraryId)
        {
            DebugLogger.log(CLOSE, "Received engine close message, starting ENGINE_CLOSE operation");
            state = ENGINE_DISCONNECT;

            attemptEngineCloseBasedLogout();
        }

        return CONTINUE;
    }

    private void attemptEngineCloseBasedLogout()
    {
        final InternalSession[] sessions = this.sessions;
        final int length = sessions.length;

        final int initialSessionLogoutIndex = this.sessionLogoutIndex;

        while (this.sessionLogoutIndex < length)
        {
            final InternalSession session = sessions[this.sessionLogoutIndex];
            final long position;
            if (session.state() == ACTIVE)
            {
                position = session.logoutAndDisconnect();
            }
            else
            {
                // Just disconnect if a logon hasn't completed at this points
                position = session.requestDisconnect();
            }

            if (position < 0)
            {
                return;
            }

            this.sessionLogoutIndex++;
        }

        // Don't repeatedly log this if you get back pressured below and re-attempt
        if (sessionLogoutIndex != initialSessionLogoutIndex && length > 0)
        {
            DebugLogger.log(CLOSE, "Completed logging out FIX Sessions");
        }

        // Continue from previous position on backpressured re-attempts
        if (iLink3LogoutIterator == null)
        {
            iLink3LogoutIterator = connectionIdToILink3Subscription.values().iterator();
        }
        while (iLink3LogoutIterator.hasNext())
        {
            final ILink3Subscription iLink3Subscription = iLink3LogoutIterator.next();
            if (Pressure.isBackPressured(iLink3Subscription.requestDisconnect(ENGINE_SHUTDOWN)))
            {
                return;
            }
        }

        if (!connectionIdToILink3Subscription.isEmpty())
        {
            DebugLogger.log(CLOSE, "Completed logging out ILink 3 Sessions");
        }

        // Yes, technically the engine is closing down, so we could flip to ATTEMPT_CONNECT state here.
        // But actually we just want to follow the normal linger and timeout procedure.
        state = CONNECTED;
    }

    private void validateEndOfDay()
    {
        if (isAtEndOfDay())
        {
            throw new IllegalStateException("Cannot perform operation whilst end of day process is running");
        }
    }

    public Action onControlNotification(
        final int libraryId,
        final InitialAcceptedSessionOwner initialAcceptedSessionOwner,
        final SessionsDecoder sessionsDecoder)
    {
        if (libraryId == this.libraryId)
        {
            final long timeInMs = timeInMs();
            livenessDetector.onHeartbeat(timeInMs);
            state = CONNECTED;
            this.initialAcceptedSessionOwner = initialAcceptedSessionOwner;
            DebugLogger.log(
                LIBRARY_CONNECT,
                controlNotificationFormatter,
                libraryId,
                timeInMs);

            controlUpdateILinkSessions();

            return controlUpdateSessions(libraryId, sessionsDecoder);
        }

        return CONTINUE;
    }

    private void controlUpdateILinkSessions()
    {
        // We just disconnect everything.
        if (iLink3Connections.length > 0)
        {
            for (final ILink3Connection session : iLink3Connections)
            {
                session.unbindState();
            }
            iLink3Connections = new ILink3Connection[0];
        }

        connectionIdToILink3Subscription.clear();
    }

    private Action controlUpdateSessions(final int libraryId, final SessionsDecoder sessionsDecoder)
    {
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

    public Action onLibraryExtendPosition(
        final int libraryId,
        final long correlationId,
        final int newSessionId,
        final long stopPosition,
        final int initialTermId,
        final int termBufferLength,
        final int mtuLength)
    {
        if (libraryId == this.libraryId &&
            // Possible to receive resent extend position responses if we resent the library connect
            // before the extend was received.
            newSessionId != outboundPublication.id())
        {
            final long timeInMs = timeInMs();
            resetNextEngineTimer(timeInMs);

            final ChannelUri channelUri = ChannelUri.parse(currentAeronChannel);
            channelUri.initialPosition(
                stopPosition,
                initialTermId,
                termBufferLength);
            RecordingCoordinator.setMtuLength(mtuLength, channelUri);
            channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(newSessionId));
            final String channel = channelUri.toString();
            DebugLogger.log(LIBRARY_CONNECT, "Extended Library Position to: ", channel);
            try
            {
                transport.newOutboundPublication(channel);
            }
            catch (final RegistrationException e)
            {
                // In this scenario we just retry again we randomly get generated a unique id
                if (!e.getMessage().contains("existing publication has clashing session id"))
                {
                    throw e;
                }
            }

            livenessDetector.onConnectStep(timeInMs);
            sendLibraryConnect(timeInMs);
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
                    return Pressure.apply(session.tryResetSequenceNumbers());
                }
            }
        }

        return Action.CONTINUE;
    }

    public Action onReplayComplete(final int libraryId, final long connection)
    {
        if (libraryId == this.libraryId)
        {
            final ILink3Subscription subscription = connectionIdToILink3Subscription.get(connection);
            if (subscription != null)
            {
                subscription.onReplayComplete();
            }

            return CONTINUE;
        }

        return CONTINUE;
    }

    // -----------------------------------------------------------------------
    //                     END EVENT HANDLERS
    // -----------------------------------------------------------------------

    private void createSessionSubscriber(
        final long connectionId,
        final InternalSession session,
        final InitiateSessionReply reply,
        final FixDictionary fixDictionary,
        final OnMessageInfo messageInfo,
        final CompositeKey compositeKey)
    {
        final MessageValidationStrategy validationStrategy = configuration.messageValidationStrategy();
        final SessionParser parser = new SessionParser(
            session, validationStrategy,
            THROW_ERRORS, configuration.validateCompIdsOnEveryMessage(), configuration.validateTimeStrictly(),
            messageInfo, sessionIdStrategy);
        parser.sessionKey(compositeKey);
        parser.fixDictionary(fixDictionary);
        final SessionSubscriber subscriber = new SessionSubscriber(
            messageInfo,
            parser,
            session,
            receiveTimer,
            sessionTimer,
            this);
        subscriber.reply(reply);
        subscriber.handler(configuration.sessionAcquireHandler().onSessionAcquired(session, sessionAcquiredInfo));

        connectionIdToSession.put(connectionId, subscriber);
    }

    private InitiatorSession newInitiatorSession(
        final long connectionId,
        final int initialSentSequenceNumber,
        final int initialReceivedSequenceNumber,
        final SessionState state,
        final int sequenceIndex,
        final boolean enableLastMsgSeqNumProcessed,
        final FixDictionary fixDictionary,
        final boolean resetSeqNum,
        final OnMessageInfo messageInfo)
    {
        final int defaultInterval = configuration.defaultHeartbeatIntervalInS();

        final MutableAsciiBuffer asciiBuffer = sessionBuffer();
        final SessionProxy sessionProxy = sessionProxy(connectionId);

        final InitiatorSession session = new InitiatorSession(
            defaultInterval,
            connectionId,
            epochClock,
            configuration.clock(),
            sessionProxy,
            inboundPublication,
            outboundPublication,
            sessionIdStrategy,
            configuration.sendingTimeWindowInMs(),
            fixCounters.receivedMsgSeqNo(connectionId),
            fixCounters.sentMsgSeqNo(connectionId),
            libraryId,
            initialSentSequenceNumber,
            sequenceIndex,
            state,
            resetSeqNum,
            configuration.reasonableTransmissionTimeInMs(),
            asciiBuffer,
            enableLastMsgSeqNumProcessed,
            configuration.sessionCustomisationStrategy(),
            messageInfo,
            epochFractionClock);
        session.fixDictionary(fixDictionary);
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

        if (sessionConfiguration.sequenceNumbersPersistent() && lastSequenceNumber != ConnectedSessionInfo.UNK_SESSION)
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
        final FixDictionary fixDictionary,
        final OnMessageInfo messageInfo)
    {
        final long sendingTimeWindow = configuration.sendingTimeWindowInMs();
        final AtomicCounter receivedMsgSeqNo = fixCounters.receivedMsgSeqNo(connectionId);
        final AtomicCounter sentMsgSeqNo = fixCounters.sentMsgSeqNo(connectionId);
        final MutableAsciiBuffer asciiBuffer = sessionBuffer();

        final InternalSession session = new AcceptorSession(
            heartbeatIntervalInS,
            connectionId,
            epochClock,
            configuration.clock(),
            sessionProxy(connectionId),
            inboundPublication,
            outboundPublication,
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
            configuration.sessionCustomisationStrategy(),
            messageInfo,
            epochFractionClock);
        session.fixDictionary(fixDictionary);
        session.address(address);
        return session;
    }

    private SessionProxy sessionProxy(final long connectionId)
    {
        return configuration.sessionProxyFactory().make(
            configuration.sessionBufferSize(),
            transport.outboundPublication(),
            sessionIdStrategy,
            configuration.sessionCustomisationStrategy(),
            new SystemEpochClock(),
            connectionId,
            libraryId,
            LangUtil::rethrowUnchecked,
            configuration.sessionEpochFractionFormat());
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
        if (state != CLOSED)
        {
            for (final WeakReference<InternalSession> ref : sessionIdToCachedSession.values())
            {
                final InternalSession session = ref.get();
                if (session != null)
                {
                    session.close();
                }
            }
            if (configuration.gracefulShutdown())
            {
                connectionIdToSession.values().forEach(subscriber -> subscriber.session().disable());
                state = CLOSED;
            }
        }
    }

    public long saveInitiateILink(final long correlationId, final ILink3ConnectionConfiguration configuration)
    {
        return outboundPublication.saveInitiateILinkConnection(
            libraryId, configuration.port(), correlationId, configuration.reEstablishLastConnection(),
            configuration.host(), configuration.accessKeyId(), configuration.useBackupHost(),
            configuration.backupHost());
    }

    void enqueueTask(final BooleanSupplier task)
    {
        tasks.add(task);
    }

    public List<ILink3Connection> iLink3Sessions()
    {
        return unmodifiableILink3Connections;
    }

    public void remove(final ILink3Connection session)
    {
        iLink3Connections = ArrayUtil.remove(iLink3Connections, session);
        connectionIdToILink3Subscription.remove(session.connectionId());
    }
}

class UnmodifiableWrapper<T> extends AbstractList<T>
{
    private final Supplier<T[]> sessions;

    UnmodifiableWrapper(final Supplier<T[]> sessions)
    {
        this.sessions = sessions;
    }

    public T get(final int index)
    {
        return sessions.get()[index];
    }

    public int size()
    {
        return sessions.get().length;
    }
}
