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
package uk.co.real_logic.fix_gateway.engine.framer;

import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.QueuedPipe;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.LivenessDetector;
import uk.co.real_logic.fix_gateway.Pressure;
import uk.co.real_logic.fix_gateway.ReliefValve;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.logger.ReplayQuery;
import uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.fix_gateway.library.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.protocol.EngineProtocolHandler;
import uk.co.real_logic.fix_gateway.protocol.EngineProtocolSubscription;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.protocol.SessionSubscription;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.timing.Timer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.net.StandardSocketOptions.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.agrona.CloseHelper.close;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.GATEWAY_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.ACCEPTOR;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.fix_gateway.messages.GatewayError.*;
import static uk.co.real_logic.fix_gateway.messages.LogonStatus.LIBRARY_NOTIFICATION;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.*;
import static uk.co.real_logic.fix_gateway.messages.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.messages.SessionState.CONNECTED;
import static uk.co.real_logic.fix_gateway.session.Session.UNKNOWN;

/**
 * Handles incoming connections from clients and outgoing connections to exchanges.
 */
public class Framer implements Agent, EngineProtocolHandler, SessionHandler
{

    private final RetryManager retryManager = new RetryManager();
    private final Int2ObjectHashMap<LibraryInfo> idToLibrary = new Int2ObjectHashMap<>();
    private final Consumer<AdminCommand> onAdminCommand = command -> command.execute(this);
    private final ReliefValve sendOutboundMessagesFunc = this::sendOutboundMessages;
    private final ReliefValve pollEndpointsFunc = () ->
    {
        try
        {
            return pollEndPoints();
        }
        catch (final IOException e)
        {
            LangUtil.rethrowUnchecked(e);
            return 0;
        }
    };

    private final PositionSender positionSender;
    private final EpochClock clock;
    private final Timer outboundTimer;
    private final Timer sendTimer;

    private final ControlledFragmentHandler outboundSubscription =
        SessionSubscription.of(this, new EngineProtocolSubscription(this));

    private final boolean hasBindAddress;
    private final Selector selector;
    private final ServerSocketChannel listeningChannel;
    private final ReceiverEndPoints receiverEndPoints = new ReceiverEndPoints();
    private final SenderEndPoints senderEndPoints;

    private final EngineConfiguration configuration;
    private final ConnectionHandler connectionHandler;
    private final Subscription outboundDataSubscription;
    private final Subscription outboundSlowSubscription;
    private final Subscription replaySubscription;
    private final GatewayPublication inboundPublication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionIds sessionIds;
    private final QueuedPipe<AdminCommand> adminCommands;
    private final SequenceNumberIndexReader sentSequenceNumberIndex;
    private final SequenceNumberIndexReader receivedSequenceNumberIndex;
    private final IdleStrategy idleStrategy;
    private final int inboundBytesReceivedLimit;
    private final int outboundLibraryFragmentLimit;
    private final int replayFragmentLimit;
    private final GatewaySessions gatewaySessions;
    private final CatchupReplayer catchupReplayer;
    private final ReplayQuery inboundMessages;
    private final ErrorHandler errorHandler;
    private final GatewayPublication outboundPublication;
    private final AtomicCounter failedCatchupSpins;
    private final AtomicCounter failedResetSessionIdSpins;

    private long nextConnectionId = (long)(Math.random() * Long.MAX_VALUE);

    public Framer(
        final EpochClock clock,
        final Timer outboundTimer,
        final Timer sendTimer,
        final EngineConfiguration configuration,
        final ConnectionHandler connectionHandler,
        final Subscription outboundLibrarySubscription,
        final Subscription outboundSlowSubscription,
        final Subscription replaySubscription,
        final QueuedPipe<AdminCommand> adminCommands,
        final SessionIdStrategy sessionIdStrategy,
        final SessionIds sessionIds,
        final SequenceNumberIndexReader sentSequenceNumberIndex,
        final SequenceNumberIndexReader receivedSequenceNumberIndex,
        final GatewaySessions gatewaySessions,
        final ReplayQuery inboundMessages,
        final ErrorHandler errorHandler,
        final GatewayPublication outboundPublication,
        final AtomicCounter failedCatchupSpins,
        final AtomicCounter failedResetSessionIdSpins)
    {
        this.clock = clock;
        this.outboundTimer = outboundTimer;
        this.sendTimer = sendTimer;
        this.configuration = configuration;
        this.connectionHandler = connectionHandler;
        this.outboundDataSubscription = outboundLibrarySubscription;
        this.replaySubscription = replaySubscription;
        this.gatewaySessions = gatewaySessions;
        this.inboundMessages = inboundMessages;
        this.errorHandler = errorHandler;
        this.outboundPublication = outboundPublication;
        this.outboundSlowSubscription = outboundSlowSubscription;
        this.failedCatchupSpins = failedCatchupSpins;
        this.failedResetSessionIdSpins = failedResetSessionIdSpins;
        this.inboundPublication = connectionHandler.inboundPublication(sendOutboundMessagesFunc);
        this.senderEndPoints = new SenderEndPoints(inboundPublication);
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionIds = sessionIds;
        this.adminCommands = adminCommands;
        this.sentSequenceNumberIndex = sentSequenceNumberIndex;
        this.receivedSequenceNumberIndex = receivedSequenceNumberIndex;
        this.idleStrategy = configuration.framerIdleStrategy();
        catchupReplayer = new CatchupReplayer(inboundPublication, errorHandler);

        this.outboundLibraryFragmentLimit = configuration.outboundLibraryFragmentLimit();
        this.replayFragmentLimit = configuration.replayFragmentLimit();
        this.inboundBytesReceivedLimit = configuration.inboundBytesReceivedLimit();
        this.hasBindAddress = configuration.hasBindAddress();

        if (hasBindAddress)
        {
            try
            {
                listeningChannel = ServerSocketChannel.open();
                listeningChannel.bind(configuration.bindAddress()).configureBlocking(false);

                selector = Selector.open();
                listeningChannel.register(selector, SelectionKey.OP_ACCEPT);
            }
            catch (final IOException ex)
            {
                throw new IllegalArgumentException(ex);
            }
        }
        else
        {
            listeningChannel = null;
            selector = null;
        }
        positionSender = new PositionSender(inboundPublication);
    }

    @Override
    public int doWork() throws Exception
    {
        final long timeInMs = clock.time();
        return retryManager.attemptSteps() +
               sendOutboundMessages() +
               sendReplayMessages() +
               pollEndPoints() +
               pollNewConnections(timeInMs) +
               pollLibraries(timeInMs) +
               gatewaySessions.pollSessions(timeInMs) +
               adminCommands.drain(onAdminCommand);
    }

    private int sendReplayMessages()
    {
        return replaySubscription.controlledPoll(outboundSubscription, replayFragmentLimit);
    }

    private int sendOutboundMessages()
    {
        final int newMessagesRead =
            outboundDataSubscription.controlledPoll(outboundSubscription, outboundLibraryFragmentLimit);
        final int messagesRead = newMessagesRead +
            outboundSlowSubscription.controlledPoll(senderEndPoints, outboundLibraryFragmentLimit);

        if (newMessagesRead > 0)
        {
            outboundDataSubscription.forEachImage(positionSender);
        }

        return messagesRead;
    }

    private int pollLibraries(final long timeInMs)
    {
        int total = 0;
        final Iterator<LibraryInfo> iterator = idToLibrary.values().iterator();
        while (iterator.hasNext())
        {
            final LibraryInfo library = iterator.next();
            total += library.poll(timeInMs);
            if (!library.isConnected())
            {
                iterator.remove();
                acquireLibrarySessions(library);
            }
        }

        return total;
    }

    private void acquireLibrarySessions(final LibraryInfo library)
    {
        final long position = outboundDataSubscription.getImage(library.aeronSessionId()).position();
        sentSequenceNumberIndex.awaitingIndexingUpTo(
            library.aeronSessionId(), position, idleStrategy);

        for (final GatewaySession session : library.gatewaySessions())
        {
            final long sessionId = session.sessionId();
            final int sentSequenceNumber = sentSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
            final int receivedSequenceNumber = receivedSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
            final boolean hasLoggedIn = receivedSequenceNumber != SessionInfo.UNKNOWN_SESSION;
            final SessionState state = hasLoggedIn ? ACTIVE : CONNECTED;
            gatewaySessions.acquire(
                session,
                state,
                session.heartbeatIntervalInS(),
                sentSequenceNumber,
                receivedSequenceNumber,
                session.username(),
                session.password()
            );
            // TODO: should backscan the gap between last received message at the engine and the library
        }
    }

    private int pollEndPoints() throws IOException
    {
        final int inboundBytesReceivedLimit = this.inboundBytesReceivedLimit;

        int totalBytesReceived = 0;
        int bytesReceived;
        do
        {
            bytesReceived = receiverEndPoints.pollEndPoints();
            totalBytesReceived += bytesReceived;
        }
        while (bytesReceived > 0 && totalBytesReceived < inboundBytesReceivedLimit);

        return totalBytesReceived;
    }

    private int pollNewConnections(final long timeInMs) throws IOException
    {
        if (!hasBindAddress)
        {
            return 0;
        }

        final int newConnections = selector.selectNow();
        if (newConnections > 0)
        {
            final Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext())
            {
                it.next();

                final SocketChannel channel = listeningChannel.accept();
                final long connectionId = this.nextConnectionId++;
                final boolean resetSequenceNumbers = configuration.acceptorSequenceNumbersResetUponReconnect();
                final GatewaySession session = setupConnection(
                        channel, connectionId, UNKNOWN, null, GATEWAY_LIBRARY_ID, ACCEPTOR, resetSequenceNumbers);

                session.disconnectAt(timeInMs + configuration.noLogonDisconnectTimeout());

                gatewaySessions.acquire(
                    session,
                    CONNECTED,
                    configuration.defaultHeartbeatIntervalInS(),
                    SessionInfo.UNKNOWN_SESSION,
                    SessionInfo.UNKNOWN_SESSION,
                    null,
                    null);

                final String address = channel.getRemoteAddress().toString();
                if (inboundPublication.saveConnect(connectionId, address) == BACK_PRESSURED)
                {
                    errorHandler.onError(new IllegalStateException(
                        "Failed to log connect from " + address + " due to backpressure"));
                }

                it.remove();
            }
        }

        return newConnections;
    }

    public Action onInitiateConnection(
        final int libraryId,
        final int port,
        final String host,
        final String senderCompId,
        final String senderSubId,
        final String senderLocationId,
        final String targetCompId,
        final SequenceNumberType sequenceNumberType,
        final int requestedInitialSequenceNumber,
        final String username,
        final String password,
        final int heartbeatIntervalInS,
        final long correlationId,
        final Header header)
    {
        final Action action = retryManager.retry(correlationId);
        if (action != null)
        {
            return action;
        }

        final LibraryInfo library = idToLibrary.get(libraryId);
        if (library == null)
        {
            saveError(GatewayError.UNKNOWN_LIBRARY, libraryId);

            return CONTINUE;
        }

        try
        {
            final SocketChannel channel;
            final InetSocketAddress address;
            try
            {
                address = new InetSocketAddress(host, port);
                channel = SocketChannel.open();
                channel.connect(address);
            }
            catch (final Exception e)
            {
                saveError(UNABLE_TO_CONNECT, libraryId, e);

                return CONTINUE;
            }

            final long connectionId = this.nextConnectionId++;

            final CompositeKey sessionKey = sessionIdStrategy.onLogon(
                senderCompId, senderSubId, senderLocationId, targetCompId);
            final long sessionId = sessionIds.onLogon(sessionKey);
            if (sessionId == SessionIds.DUPLICATE_SESSION)
            {
                saveError(DUPLICATE_SESSION, libraryId);

                return CONTINUE;
            }

            final boolean resetSeqNumbers = sequenceNumberType == SequenceNumberType.TRANSIENT;
            final GatewaySession session =
                setupConnection(channel, connectionId, sessionId, sessionKey, libraryId, INITIATOR, resetSeqNumbers);

            library.addSession(session);

            sentSequenceNumberIndex.awaitingIndexingUpTo(header, idleStrategy);

            final int lastSentSequenceNumber = sentSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
            final int lastReceivedSequenceNumber = receivedSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
            session.onLogon(sessionId, sessionKey, username, password, heartbeatIntervalInS);

            final Transaction transaction = new Transaction(
                () -> inboundPublication.saveManageConnection(
                    connectionId, address.toString(), libraryId, INITIATOR,
                    lastSentSequenceNumber, lastReceivedSequenceNumber, CONNECTED, heartbeatIntervalInS),
                () -> inboundPublication.saveLogon(
                    libraryId, connectionId, sessionId,
                    lastSentSequenceNumber, lastReceivedSequenceNumber,
                    senderCompId, senderSubId, senderLocationId, targetCompId,
                    username, password, LogonStatus.NEW)
            );

            return retryManager.firstAttempt(correlationId, transaction);
        }
        catch (final Exception e)
        {
            saveError(EXCEPTION, libraryId, e);
        }

        return CONTINUE;
    }

    private void saveError(final GatewayError error, final int libraryId)
    {
        final long position = inboundPublication.saveError(error, libraryId, "");
        pressuredError(error, libraryId, null, position);
    }

    private void saveError(final GatewayError error, final int libraryId, final Exception e)
    {
        final String message = e.getMessage();
        final long position = inboundPublication.saveError(error, libraryId, message == null ? "" : message);
        pressuredError(error, libraryId, message, position);
    }

    private void pressuredError(
        final GatewayError error, final int libraryId, final String message, final long position)
    {
        if (position == BACK_PRESSURED)
        {
            if (message == null)
            {
                errorHandler.onError(new IllegalStateException(
                    "Back pressured " + error + " for " + libraryId));
            }
            else
            {
                errorHandler.onError(new IllegalStateException(
                    "Back pressured " + error + ": " + message + " for " + libraryId));
            }
        }
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
        final long now = outboundTimer.recordSince(timestamp);

        senderEndPoints.onMessage(connectionId, buffer, offset, length);

        sendTimer.recordSince(now);

        return CONTINUE;
    }

    private GatewaySession setupConnection(
        final SocketChannel channel,
        final long connectionId,
        final long sessionId,
        final CompositeKey sessionKey,
        final int libraryId,
        final ConnectionType connectionType,
        final boolean resetSequenceNumbers)
        throws IOException
    {
        channel.setOption(TCP_NODELAY, true);
        if (configuration.receiverSocketBufferSize() > 0)
        {
            channel.setOption(SO_RCVBUF, configuration.receiverSocketBufferSize());
        }
        if (configuration.senderSocketBufferSize() > 0)
        {
            channel.setOption(SO_SNDBUF, configuration.senderSocketBufferSize());
        }
        channel.configureBlocking(false);

        final ReceiverEndPoint receiverEndPoint =
            connectionHandler.receiverEndPoint(channel, connectionId, sessionId, libraryId, this,
                sendOutboundMessagesFunc, sentSequenceNumberIndex, receivedSequenceNumberIndex, resetSequenceNumbers);
        receiverEndPoints.add(receiverEndPoint);

        final SenderEndPoint senderEndPoint =
            connectionHandler.senderEndPoint(channel, connectionId, libraryId, this);
        senderEndPoints.add(senderEndPoint);

        final GatewaySession gatewaySession = new GatewaySession(
            connectionId,
            sessionId,
            channel.getRemoteAddress().toString(),
            connectionType,
            sessionKey,
            receiverEndPoint,
            senderEndPoint
        );

        receiverEndPoint.gatewaySession(gatewaySession);

        return gatewaySession;
    }

    public Action onRequestDisconnect(final int libraryId, final long connectionId)
    {
        return onDisconnect(libraryId, connectionId, null);
    }

    public Action onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        receiverEndPoints.removeConnection(connectionId);
        senderEndPoints.removeConnection(connectionId);
        final LibraryInfo library = idToLibrary.get(libraryId);
        if (library != null)
        {
            library.removeSession(connectionId);
        }
        else
        {
            gatewaySessions.release(connectionId);
        }

        return CONTINUE;
    }

    public Action onLibraryConnect(final int libraryId, final int aeronSessionId)
    {
        final long timeInMs = clock.time();
        if (idToLibrary.containsKey(libraryId))
        {
            saveError(DUPLICATE_LIBRARY_ID, libraryId);

            return CONTINUE;
        }

        final LivenessDetector livenessDetector = LivenessDetector.forEngine(
            inboundPublication,
            libraryId,
            configuration.replyTimeoutInMs(),
            timeInMs);

        final LibraryInfo library = new LibraryInfo(libraryId, livenessDetector, aeronSessionId);
        idToLibrary.put(libraryId, library);

        for (final GatewaySession gatewaySession : gatewaySessions.sessions())
        {
            saveLogon(libraryId, gatewaySession, SessionInfo.UNKNOWN_SESSION, SessionInfo.UNKNOWN_SESSION,
                LIBRARY_NOTIFICATION);
        }

        return CONTINUE;
    }

    public Action onApplicationHeartbeat(final int libraryId)
    {
        final LibraryInfo library = idToLibrary.get(libraryId);
        if (library != null)
        {
            final long timeInMs = clock.time();
            library.onHeartbeat(timeInMs);
        }

        return CONTINUE;
    }

    public Action onReleaseSession(
        final int libraryId,
        final long connectionId,
        final long correlationId,
        final SessionState state,
        final long heartbeatIntervalInMs,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final String username,
        final String password,
        final Header header)
    {
        final LibraryInfo libraryInfo = idToLibrary.get(libraryId);
        if (libraryInfo == null)
        {
            inboundPublication.saveReleaseSessionReply(SessionReplyStatus.UNKNOWN_LIBRARY, correlationId);

            return CONTINUE;
        }

        final GatewaySession session = libraryInfo.removeSession(connectionId);
        if (session == null)
        {
            inboundPublication.saveReleaseSessionReply(SessionReplyStatus.UNKNOWN_SESSION, correlationId);

            return CONTINUE;
        }

        gatewaySessions.acquire(
            session,
            state,
            (int) MILLISECONDS.toSeconds(heartbeatIntervalInMs),
            lastSentSequenceNumber,
            lastReceivedSequenceNumber,
            username,
            password);

        inboundPublication.saveReleaseSessionReply(OK, correlationId);

        return CONTINUE;
    }

    public Action onRequestSession(
        final int libraryId,
        final long sessionId,
        final long correlationId,
        int replayFromSequenceNumber)
    {
        final LibraryInfo libraryInfo = idToLibrary.get(libraryId);
        if (libraryInfo == null)
        {
            return Pressure.apply(
                inboundPublication.saveRequestSessionReply(SessionReplyStatus.UNKNOWN_LIBRARY, correlationId));
        }

        final GatewaySession gatewaySession = gatewaySessions.release(sessionId);
        if (gatewaySession == null)
        {
            return Pressure.apply(
                inboundPublication.saveRequestSessionReply(SessionReplyStatus.UNKNOWN_SESSION, correlationId));
        }

        final long connectionId = gatewaySession.connectionId();
        final Session session = gatewaySession.session();
        final int lastSentSeqNum = session.lastSentMsgSeqNum();
        final int lastRecvSeqNum = session.lastReceivedMsgSeqNum();
        final SessionState sessionState = session.state();
        gatewaySession.handoverManagementTo(libraryId);
        libraryInfo.addSession(gatewaySession);

        // TODO: ensure all or none of these messages get through.
        inboundPublication.saveManageConnection(
            connectionId,
            gatewaySession.address(),
            libraryId,
            gatewaySession.connectionType(),
            lastSentSeqNum,
            lastRecvSeqNum,
            sessionState,
            gatewaySession.heartbeatIntervalInS());

        saveLogon(libraryId, gatewaySession, lastSentSeqNum, lastRecvSeqNum, LogonStatus.NEW);

        if (catchupSession(libraryId, connectionId, correlationId, replayFromSequenceNumber, session, lastRecvSeqNum))
        {
            inboundPublication.saveRequestSessionReply(OK, correlationId);
        }

        return CONTINUE;
    }

    private void saveLogon(final int libraryId,
                           final GatewaySession gatewaySession,
                           final int lastSentSeqNum,
                           final int lastReceivedSeqNum,
                           final LogonStatus status)
    {
        final CompositeKey compositeKey = gatewaySession.compositeKey();
        if (compositeKey != null)
        {
            final long connectionId = gatewaySession.connectionId();
            final String username = gatewaySession.username();
            final String password = gatewaySession.password();
            inboundPublication.saveLogon(
                libraryId,
                connectionId,
                gatewaySession.sessionId(),
                lastSentSeqNum,
                lastReceivedSeqNum,
                compositeKey.senderCompId(),
                compositeKey.senderSubId(),
                compositeKey.senderLocationId(),
                compositeKey.targetCompId(),
                username,
                password,
                status);
        }
    }

    private boolean catchupSession(final int libraryId,
                                   final long connectionId,
                                   final long correlationId,
                                   final int replayFromSequenceNumber,
                                   final Session session,
                                   final int lastReceivedSeqNum)
    {
        if (replayFromSequenceNumber != NO_MESSAGE_REPLAY)
        {
            final long sessionId = session.id();
            if (sessionId == Session.UNKNOWN)
            {
                inboundPublication.saveRequestSessionReply(SESSION_NOT_LOGGED_IN, correlationId);
                return false;
            }
            final int expectedNumberOfMessages = lastReceivedSeqNum - replayFromSequenceNumber;

            if (expectedNumberOfMessages < 0)
            {
                sequenceNumberTooHigh(correlationId, replayFromSequenceNumber, lastReceivedSeqNum);
                return false;
            }

            inboundPublication.saveCatchup(libraryId, connectionId, expectedNumberOfMessages);

            while (receivedSequenceNumberIndex.lastKnownSequenceNumber(sessionId) < lastReceivedSeqNum)
            {
                idleStrategy.idle();
                failedCatchupSpins.increment();
            }
            idleStrategy.reset();

            catchupReplayer.libraryId(libraryId);
            final int numberOfMessages =
                inboundMessages.query(
                    catchupReplayer,
                    sessionId,
                    replayFromSequenceNumber + 1, // convert to inclusive numbering
                    lastReceivedSeqNum);

            if (numberOfMessages < expectedNumberOfMessages)
            {
                missingMessages(correlationId, expectedNumberOfMessages, numberOfMessages);
                return false;
            }
        }

        return true;
    }

    private void missingMessages(final long correlationId,
                                 final int expectedNumberOfMessages,
                                 final int numberOfMessages)
    {
        errorHandler.onError(new IllegalStateException(String.format(
            "Failed to read correct number of messages for %d, expected %d, read %d",
            correlationId,
            expectedNumberOfMessages,
            numberOfMessages)));
        inboundPublication.saveRequestSessionReply(MISSING_MESSAGES, correlationId);
    }

    private void sequenceNumberTooHigh(final long correlationId,
                                       final int replayFromSequenceNumber,
                                       final int lastReceivedSeqNum)
    {
        errorHandler.onError(new IllegalStateException(String.format(
            "Sequence Number too high for %d, wanted %d, but we've only archived %d",
            correlationId,
            replayFromSequenceNumber,
            lastReceivedSeqNum)));
        inboundPublication.saveRequestSessionReply(SEQUENCE_NUMBER_TOO_HIGH, correlationId);
    }

    void onQueryLibraries(final QueryLibrariesCommand command)
    {
        final List<LibraryInfo> libraries = new ArrayList<>(idToLibrary.values());
        command.success(libraries);
    }

    void onGatewaySessions(final GatewaySessionsCommand command)
    {
        command.success(new ArrayList<>(gatewaySessions.sessions()));
    }

    void onResetSessionIds(final File backupLocation, final ResetSessionIdsCommand command)
    {
        try
        {
            inboundPublication.saveResetSessionIds();
            outboundPublication.saveResetSessionIds();
            sessionIds.reset(backupLocation);
            while (sequenceNumbersNotReset())
            {
                idleStrategy.idle();
                failedResetSessionIdSpins.increment();
            }
            idleStrategy.reset();

            command.success();
        }
        catch (final Exception ex)
        {
            command.onError(ex);
        }
    }

    private boolean sequenceNumbersNotReset()
    {
        return sentSequenceNumberIndex.lastKnownSequenceNumber(1) != SessionInfo.UNKNOWN_SESSION
            || receivedSequenceNumberIndex.lastKnownSequenceNumber(1) != SessionInfo.UNKNOWN_SESSION;
    }

    public void onClose()
    {
        inboundMessages.close();
        receiverEndPoints.close();
        senderEndPoints.close();
        close(selector);
        close(listeningChannel);
    }

    public String roleName()
    {
        return "Framer";
    }

    void schedule(final Step step)
    {
        if (step.attempt() < 0)
        {
            retryManager.schedule(step);
        }
    }
}
