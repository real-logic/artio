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
package uk.co.real_logic.artio.engine.framer;

import io.aeron.ControlledFragmentAssembler;
import io.aeron.Image;
import io.aeron.ImageControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2LongHashMap.KeyIterator;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.QueuedPipe;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LivenessDetector;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.*;
import uk.co.real_logic.artio.engine.framer.SubscriptionSlowPeeker.LibrarySlowPeeker;
import uk.co.real_logic.artio.engine.framer.TcpChannelSupplier.NewChannelHandler;
import uk.co.real_logic.artio.engine.logger.ReplayQuery;
import uk.co.real_logic.artio.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.protocol.*;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.InternalSession;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.session.SessionIdStrategy;
import uk.co.real_logic.artio.timing.Timer;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.agrona.collections.CollectionUtil.removeIf;
import static uk.co.real_logic.artio.GatewayProcess.NO_CONNECTION_ID;
import static uk.co.real_logic.artio.GatewayProcess.NO_CORRELATION_ID;
import static uk.co.real_logic.artio.LogTag.*;
import static uk.co.real_logic.artio.Pressure.isBackPressured;
import static uk.co.real_logic.artio.dictionary.generation.Exceptions.closeAll;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.engine.InitialAcceptedSessionOwner.SOLE_LIBRARY;
import static uk.co.real_logic.artio.engine.SessionInfo.UNK_SESSION;
import static uk.co.real_logic.artio.engine.framer.Continuation.COMPLETE;
import static uk.co.real_logic.artio.engine.framer.GatewaySession.adjustLastSequenceNumber;
import static uk.co.real_logic.artio.engine.framer.SessionContexts.UNKNOWN_SESSION;
import static uk.co.real_logic.artio.library.FixLibrary.CURRENT_SEQUENCE;
import static uk.co.real_logic.artio.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.artio.messages.ConnectionType.ACCEPTOR;
import static uk.co.real_logic.artio.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.artio.messages.GatewayError.*;
import static uk.co.real_logic.artio.messages.SequenceNumberType.PERSISTENT;
import static uk.co.real_logic.artio.messages.SequenceNumberType.TRANSIENT;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.*;
import static uk.co.real_logic.artio.messages.SessionState.ACTIVE;
import static uk.co.real_logic.artio.messages.SessionState.CONNECTED;
import static uk.co.real_logic.artio.messages.SessionStatus.LIBRARY_NOTIFICATION;

/**
 * Handles incoming connections from clients and outgoing connections to exchanges.
 */
class Framer implements Agent, EngineEndPointHandler, ProtocolHandler
{

    private final RetryManager retryManager = new RetryManager();
    private final List<ResetSequenceNumberCommand> replies = new ArrayList<>();
    private final Int2ObjectHashMap<LiveLibraryInfo> idToLibrary = new Int2ObjectHashMap<>();
    private final List<LiveLibraryInfo> librariesBeingAcquired = new ArrayList<>();
    private final Consumer<AdminCommand> onAdminCommand = command -> command.execute(this);
    private final NewChannelHandler onNewConnectionFunc = this::onNewConnection;
    private final Predicate<LiveLibraryInfo> retryAcquireLibrarySessionsFunc = this::retryAcquireLibrarySessions;

    private final TcpChannelSupplier channelSupplier;
    private final EpochClock epochClock;
    private final Timer outboundTimer;
    private final Timer sendTimer;

    private final ControlledFragmentHandler librarySubscriber;
    private final ControlledFragmentHandler replaySubscriber;
    private final ControlledFragmentHandler replaySlowSubscriber;

    private final ReceiverEndPoints receiverEndPoints;
    private final ControlledFragmentAssembler senderEndPointAssembler;
    private final SenderEndPoints senderEndPoints;

    private final EngineConfiguration configuration;
    private final EndPointFactory endPointFactory;
    private final Subscription librarySubscription;
    private final SubscriptionSlowPeeker librarySlowPeeker;
    private final Image replayImage;
    private final SlowPeeker replaySlowPeeker;
    private final BlockablePosition engineBlockablePosition;
    private final GatewayPublication inboundPublication;
    private final String agentNamePrefix;
    private final CompletionPosition inboundCompletionPosition;
    private final CompletionPosition outboundLibraryCompletionPosition;
    private final FinalImagePositions finalImagePositions;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionContexts sessionContexts;
    private final QueuedPipe<AdminCommand> adminCommands;
    private final SequenceNumberIndexReader sentSequenceNumberIndex;
    private final SequenceNumberIndexReader receivedSequenceNumberIndex;
    private final int inboundBytesReceivedLimit;
    private final int outboundLibraryFragmentLimit;
    private final int replayFragmentLimit;
    private final GatewaySessions gatewaySessions;
    private final Consumer<GatewaySession> onSessionlogon = this::onSessionLogon;

    /**
     * Null if inbound messages are not logged
     */
    private final ReplayQuery inboundMessages;
    private final ErrorHandler errorHandler;
    private final GatewayPublication outboundPublication;
    // Both connection id to library id maps
    private final Long2LongHashMap resendSlowStatus = new Long2LongHashMap(-1);
    private final Long2LongHashMap resendNotSlowStatus = new Long2LongHashMap(-1);
    private final AgentInvoker conductorAgentInvoker;
    private final RecordingCoordinator recordingCoordinator;
    private final PositionSender nonLoggingPositionSender;

    private final SessionHeaderDecoder acceptorHeaderDecoder;
    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final boolean soleLibraryMode;

    private long nextConnectionId = (long)(Math.random() * Long.MAX_VALUE);

    private boolean performingCloseOperation = false;

    Framer(
        final EpochClock epochClock,
        final Timer outboundTimer,
        final Timer sendTimer,
        final EngineConfiguration configuration,
        final EndPointFactory endPointFactory,
        final Subscription librarySubscription,
        final Subscription slowSubscription,
        final Image replayImage,
        final Image replaySlowImage,
        final ReplayQuery inboundMessages,
        final GatewayPublication outboundPublication,
        final GatewayPublication inboundPublication,
        final QueuedPipe<AdminCommand> adminCommands,
        final SessionIdStrategy sessionIdStrategy,
        final SessionContexts sessionContexts,
        final SequenceNumberIndexReader sentSequenceNumberIndex,
        final SequenceNumberIndexReader receivedSequenceNumberIndex,
        final GatewaySessions gatewaySessions,
        final ErrorHandler errorHandler,
        final String agentNamePrefix,
        final CompletionPosition inboundCompletionPosition,
        final CompletionPosition outboundLibraryCompletionPosition,
        final FinalImagePositions finalImagePositions,
        final AgentInvoker conductorAgentInvoker,
        final RecordingCoordinator recordingCoordinator)
    {
        this.epochClock = epochClock;
        this.outboundTimer = outboundTimer;
        this.sendTimer = sendTimer;
        this.configuration = configuration;
        this.endPointFactory = endPointFactory;
        this.librarySubscription = librarySubscription;
        this.replayImage = replayImage;
        this.gatewaySessions = gatewaySessions;
        this.inboundMessages = inboundMessages;
        this.errorHandler = errorHandler;
        this.outboundPublication = outboundPublication;
        this.inboundPublication = inboundPublication;
        this.agentNamePrefix = agentNamePrefix;
        this.inboundCompletionPosition = inboundCompletionPosition;
        this.outboundLibraryCompletionPosition = outboundLibraryCompletionPosition;
        this.senderEndPoints = new SenderEndPoints(errorHandler);
        this.conductorAgentInvoker = conductorAgentInvoker;
        this.recordingCoordinator = recordingCoordinator;
        this.senderEndPointAssembler = new ControlledFragmentAssembler(senderEndPoints, 0, true);
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionContexts = sessionContexts;
        this.adminCommands = adminCommands;
        this.sentSequenceNumberIndex = sentSequenceNumberIndex;
        this.receivedSequenceNumberIndex = receivedSequenceNumberIndex;
        this.finalImagePositions = finalImagePositions;
        this.soleLibraryMode = configuration.initialAcceptedSessionOwner() == SOLE_LIBRARY;

        acceptorHeaderDecoder = configuration.acceptorfixDictionary().makeHeaderDecoder();

        receiverEndPoints = new ReceiverEndPoints(errorHandler);

        this.librarySlowPeeker = new SubscriptionSlowPeeker(slowSubscription, librarySubscription);

        this.outboundLibraryFragmentLimit = configuration.outboundLibraryFragmentLimit();
        this.replayFragmentLimit = configuration.replayFragmentLimit();
        this.inboundBytesReceivedLimit = configuration.inboundBytesReceivedLimit();

        this.replaySlowPeeker = new SlowPeeker(replaySlowImage, replayImage);
        endPointFactory.replaySlowPeeker(replaySlowPeeker);


        engineBlockablePosition = getOutboundSlowPeeker(outboundPublication);
        librarySubscriber = new ControlledFragmentAssembler(
            ProtocolSubscription.of(this, new EngineProtocolSubscription(this)),
            0,
            true);
        nonLoggingPositionSender = configuration.logOutboundMessages() ?
            null : new PositionSender(inboundPublication);

        // We lookup replayed message by session id, since the connection id may have changed
        // if it's a persistent session.
        replaySubscriber = new ImageControlledFragmentAssembler(ProtocolSubscription.of(new ProtocolHandler()
        {
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
                final long position)
            {
                return senderEndPoints.onReplayMessage(connectionId, buffer, offset, length, position);
            }

            public Action onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
            {
                // Should never be replayed.
                return Action.CONTINUE;
            }
        },
        new ReplayProtocolSubscription(senderEndPoints::onReplayComplete)),
        0,
        true);

        replaySlowSubscriber = new ControlledFragmentAssembler(ProtocolSubscription.of(new ProtocolHandler()
        {
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
                final long position)
            {
                return senderEndPoints.onSlowReplayMessage(connectionId, buffer, offset, length, position);
            }

            public Action onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
            {
                // Should never be replayed.
                return Action.CONTINUE;
            }
        },
        new ReplayProtocolSubscription(senderEndPoints::onReplayComplete)));

        channelSupplier = configuration.channelSupplier();
    }

    private LibrarySlowPeeker getOutboundSlowPeeker(final GatewayPublication outboundPublication)
    {
        final int outboundSessionId = outboundPublication.id();
        LibrarySlowPeeker outboundSlowPeeker;
        while ((outboundSlowPeeker = this.librarySlowPeeker.addLibrary(outboundSessionId)) == null)
        {
            if (conductorAgentInvoker != null)
            {
                conductorAgentInvoker.invoke();
            }

            Thread.yield();
        }

        return outboundSlowPeeker;
    }

    public int doWork() throws Exception
    {
        final long timeInMs = epochClock.time();
        senderEndPoints.timeInMs(timeInMs);
        return retryManager.attemptSteps() +
            sendOutboundMessages() +
            sendReplayMessages() +
            pollEndPoints() +
            pollNewConnections(timeInMs) +
            pollLibraries(timeInMs) +
            gatewaySessions.pollSessions(timeInMs) +
            senderEndPoints.checkTimeouts(timeInMs) +
            adminCommands.drain(onAdminCommand) +
            checkDutyCycle();
    }

    private int checkDutyCycle()
    {
        return removeIf(replies, ResetSequenceNumberCommand::poll) +
            resendSaveNotifications(this.resendSlowStatus, SlowStatus.SLOW) +
            resendSaveNotifications(this.resendNotSlowStatus, SlowStatus.NOT_SLOW);
    }

    private int resendSaveNotifications(final Long2LongHashMap resend, final SlowStatus status)
    {
        int actions = 0;
        if (!resend.isEmpty())
        {
            final KeyIterator keyIterator = resend.keySet().iterator();
            while (keyIterator.hasNext())
            {
                final long connectionId = keyIterator.nextValue();
                final int libraryId = (int)resend.get(connectionId);
                final long position = inboundPublication.saveSlowStatusNotification(
                    libraryId, connectionId, status);
                if (position > 0)
                {
                    actions++;
                    keyIterator.remove();
                }
            }
        }

        return actions;
    }

    private int sendReplayMessages()
    {
        return replayImage.controlledPoll(replaySubscriber, replayFragmentLimit) +
            replaySlowPeeker.peek(replaySlowSubscriber);
    }

    private int sendOutboundMessages()
    {
        int messagesRead = librarySubscription.controlledPoll(librarySubscriber, outboundLibraryFragmentLimit);
        messagesRead += librarySlowPeeker.peek(senderEndPointAssembler);

        if (nonLoggingPositionSender != null)
        {
            nonLoggingPositionSender.doWork();
        }

        return messagesRead;
    }

    private int pollLibraries(final long timeInMs)
    {
        int total = 0;
        final Iterator<LiveLibraryInfo> iterator = idToLibrary.values().iterator();
        while (iterator.hasNext())
        {
            final LiveLibraryInfo library = iterator.next();
            total += library.poll(timeInMs);
            if (!library.isConnected())
            {
                DebugLogger.log(LIBRARY_MANAGEMENT, "Timing out connection to library %s%n", library.libraryId());

                iterator.remove();
                library.releaseSlowPeeker();
                tryAcquireLibrarySessions(library);
                saveLibraryTimeout(library);
            }
        }

        total += removeIf(librariesBeingAcquired, retryAcquireLibrarySessionsFunc);

        return total;
    }

    private void tryAcquireLibrarySessions(final LiveLibraryInfo library)
    {
        final int librarySessionId = library.aeronSessionId();
        final Image image = librarySubscription.imageBySessionId(librarySessionId);
        long libraryPosition = finalImagePositions.lookupPosition(librarySessionId);
        if (image != null)
        {
            libraryPosition = image.position();
        }

        final boolean indexed = sentIndexedPosition(librarySessionId, libraryPosition);
        if (!configuration.logOutboundMessages() || indexed)
        {
            acquireLibrarySessions(library);
        }
        else
        {
            library.acquireAtPosition(libraryPosition);
            librariesBeingAcquired.add(library);
        }
    }

    private boolean retryAcquireLibrarySessions(final LiveLibraryInfo library)
    {
        final boolean indexed = sentIndexedPosition(library.aeronSessionId(), library.acquireAtPosition());
        if (!configuration.logOutboundMessages() || indexed)
        {
            acquireLibrarySessions(library);
        }

        return indexed;
    }

    private boolean sentIndexedPosition(final int aeronSessionId, final long position)
    {
        final long indexedPosition = sentSequenceNumberIndex.indexedPosition(aeronSessionId);
        return indexedPosition >= position;
    }

    private void saveLibraryTimeout(final LibraryInfo library)
    {
        final int libraryId = library.libraryId();
        schedule(() -> inboundPublication.saveLibraryTimeout(libraryId, 0));
        schedule(() -> outboundPublication.saveLibraryTimeout(libraryId, 0));
    }

    private void acquireLibrarySessions(final LiveLibraryInfo library)
    {
        final List<GatewaySession> sessions = library.gatewaySessions();
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final GatewaySession session = sessions.get(i);
            final long sessionId = session.sessionId();
            final int sentSequenceNumber = sentSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
            final int receivedSequenceNumber = receivedSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
            final boolean hasLoggedIn = receivedSequenceNumber != UNK_SESSION;
            final SessionState state = hasLoggedIn ? ACTIVE : CONNECTED;

            DebugLogger.log(
                LIBRARY_MANAGEMENT,
                "Acquiring session %s from library %s%n", session.sessionId(), library.libraryId());

            gatewaySessions.acquire(
                session,
                state,
                false,
                session.heartbeatIntervalInS(),
                sentSequenceNumber,
                receivedSequenceNumber,
                session.username(),
                session.password(),
                engineBlockablePosition);

            schedule(() -> saveManageSession(
                ENGINE_LIBRARY_ID,
                session,
                sentSequenceNumber,
                receivedSequenceNumber,
                SessionStatus.LIBRARY_NOTIFICATION));

            if (performingCloseOperation)
            {
                session.session().logoutAndDisconnect();
            }
        }

        finalImagePositions.removePosition(library.aeronSessionId());
    }

    private int pollEndPoints()
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
        return channelSupplier.pollSelector(timeInMs, onNewConnectionFunc);
    }

    private void onNewConnection(final long timeInMs, final TcpChannel channel)
    {
        if (performingCloseOperation)
        {
            channel.close();
            return;
        }

        final long connectionId = newConnectionId();
        final GatewaySession gatewaySession = setupConnection(
            channel,
            connectionId,
            UNKNOWN_SESSION,
            null,
            ENGINE_LIBRARY_ID,
            ACCEPTOR,
            configuration.acceptedSessionClosedResendInterval(),
            configuration.acceptedSessionResendRequestChunkSize(),
            configuration.acceptedSessionSendRedundantResendRequests(),
            configuration.acceptedEnableLastMsgSeqNumProcessed(),
            configuration.acceptorfixDictionary());

        gatewaySession.disconnectAt(timeInMs + configuration.noLogonDisconnectTimeoutInMs());

        // In sole library mode we forward all connections to the sole library
        if (soleLibraryMode)
        {
            gatewaySessions.track(gatewaySession);
        }
        else
        {
            gatewaySessions.acquire(
                gatewaySession,
                CONNECTED,
                false,
                configuration.defaultHeartbeatIntervalInS(),
                UNK_SESSION,
                UNK_SESSION,
                null,
                null,
                engineBlockablePosition);
        }

        final String address = channel.remoteAddress();
        // In this case the save connect is simply logged for posterities sake
        // So in the back-pressure we should just drop it
        final long position = inboundPublication.saveConnect(connectionId, address);
        if (isBackPressured(position))
        {
            errorHandler.onError(new IllegalStateException(
                "Failed to log connect from " + address + " due to backpressure"));
        }
    }

    private long newConnectionId()
    {
        long connectionId;
        do
        {
            connectionId = this.nextConnectionId++;
        }
        while (connectionId == NO_CONNECTION_ID);

        return connectionId;
    }

    public Action onInitiateConnection(
        final int libraryId,
        final int port,
        final String host,
        final String senderCompId,
        final String senderSubId,
        final String senderLocationId,
        final String targetCompId,
        final String targetSubId,
        final String targetLocationId,
        final SequenceNumberType sequenceNumberType,
        final int requestedInitialReceivedSequenceNumber,
        final int requestedInitialSentSequenceNumber,
        final boolean resetSequenceNumber,
        final boolean closedResendInterval,
        final int resendRequestChunkSize,
        final boolean sendRedundantResendRequests,
        final boolean enableLastMsgSeqNumProcessed,
        final String username,
        final String password,
        final Class<? extends FixDictionary> fixDictionary,
        final int heartbeatIntervalInS,
        final long correlationId,
        final Header header)
    {
        final LiveLibraryInfo library = idToLibrary.get(libraryId);
        if (library == null)
        {
            saveError(GatewayError.UNKNOWN_LIBRARY, libraryId, correlationId, "");

            return CONTINUE;
        }

        final boolean logInboundMessages = configuration.logInboundMessages();
        final boolean logOutboundMessages = configuration.logOutboundMessages();
        if (sequenceNumberType == PERSISTENT && !configuration.logAllMessages())
        {
            return badSequenceNumberConfiguration(libraryId, correlationId, logInboundMessages, logOutboundMessages);
        }

        final CompositeKey sessionKey = sessionIdStrategy.onInitiateLogon(
            senderCompId,
            senderSubId,
            senderLocationId,
            targetCompId,
            targetSubId,
            targetLocationId);

        final SessionContext sessionContext = sessionContexts.onLogon(sessionKey);

        if (sessionContext == SessionContexts.DUPLICATE_SESSION)
        {
            final long sessionId = sessionContexts.lookupSessionId(sessionKey);
            final int owningLibraryId = senderEndPoints.libraryLookup().applyAsInt(sessionId);
            final String msg =
                "Duplicate Session for: " + sessionKey +
                " Surrogate Key: " + sessionId +
                " Currently owned by " + owningLibraryId;

            saveError(DUPLICATE_SESSION, libraryId, correlationId, msg);

            return CONTINUE;
        }

        try
        {
            DebugLogger.log(
                FIX_CONNECTION,
                "Connecting to %s:%d from library %d%n", host, port, libraryId);

            final InetSocketAddress address = new InetSocketAddress(host, port);
            final ConnectingSession connectingSession = new ConnectingSession(address, sessionContext.sessionId());
            library.connectionStartsConnecting(correlationId, connectingSession);
            channelSupplier.open(address,
                (channel, ex) ->
                {
                    if (ex != null)
                    {
                        sessionContexts.onDisconnect(sessionContext.sessionId());
                        library.connectionFinishesConnecting(correlationId);
                        saveError(UNABLE_TO_CONNECT, libraryId, correlationId, ex);
                        return;
                    }

                    onConnectionOpen(
                        libraryId,
                        senderCompId,
                        senderSubId,
                        senderLocationId,
                        targetCompId,
                        targetSubId,
                        targetLocationId,
                        sequenceNumberType,
                        resetSequenceNumber,
                        closedResendInterval,
                        resendRequestChunkSize,
                        sendRedundantResendRequests,
                        enableLastMsgSeqNumProcessed,
                        username,
                        password,
                        fixDictionary,
                        heartbeatIntervalInS,
                        correlationId,
                        header,
                        library,
                        address,
                        channel,
                        sessionContext,
                        sessionKey);
                });
        }
        catch (final Exception ex)
        {
            sessionContexts.onDisconnect(sessionContext.sessionId());
            saveError(UNABLE_TO_CONNECT, libraryId, correlationId, ex);

            return CONTINUE;
        }

        return CONTINUE;
    }

    public Action onMidConnectionDisconnect(final int libraryId, final long correlationId)
    {
        final LiveLibraryInfo library = idToLibrary.get(libraryId);
        if (library == null)
        {
            saveError(GatewayError.UNKNOWN_LIBRARY, libraryId, correlationId, "");

            return CONTINUE;
        }

        final ConnectingSession connectingSession = library.connectionFinishesConnecting(correlationId);
        if (connectingSession == null)
        {
            saveError(GatewayError.UNKNOWN_SESSION, libraryId, correlationId,
                "Engine doesn't think library is connecting this session");

            return CONTINUE;
        }

        sessionContexts.onDisconnect(connectingSession.sessionId());
        try
        {
            channelSupplier.stopConnecting(connectingSession.address());
        }
        catch (final IOException e)
        {
            errorHandler.onError(e);
        }

        return CONTINUE;
    }

    private Action badSequenceNumberConfiguration(
        final int libraryId,
        final long correlationId,
        final boolean logInboundMessages,
        final boolean logOutboundMessages)
    {
        final String msg =
            "You need to enable the logging of inbound and outbound messages on your EngineConfiguration" +
            "in order to initiate a connection with persistent sequence numbers. " +
            "logInboundMessages = " + logInboundMessages +
            "logOutboundMessages = " + logOutboundMessages;

        saveError(INVALID_CONFIGURATION, libraryId, correlationId, msg);

        return CONTINUE;
    }

    private void onConnectionOpen(
        final int libraryId,
        final String senderCompId,
        final String senderSubId,
        final String senderLocationId,
        final String targetCompId,
        final String targetSubId,
        final String targetLocationId,
        final SequenceNumberType sequenceNumberType,
        final boolean resetSequenceNumber,
        final boolean closedResendInterval,
        final int resendRequestChunkSize,
        final boolean sendRedundantResendRequests,
        final boolean enableLastMsgSeqNumProcessed,
        final String username,
        final String password,
        final Class<? extends FixDictionary> fixDictionary,
        final int heartbeatIntervalInS,
        final long correlationId,
        final Header header,
        final LiveLibraryInfo library,
        final InetSocketAddress address,
        final TcpChannel channel,
        final SessionContext sessionContext,
        final CompositeKey sessionKey)
    {
        try
        {
            DebugLogger.log(FIX_CONNECTION,
                "Initiating session %s from library %s%n", sessionContext.sessionId(), library.libraryId());
            final long connectionId = newConnectionId();
            sessionContext.onLogon(resetSequenceNumber || sequenceNumberType == TRANSIENT);
            final long sessionId = sessionContext.sessionId();
            final GatewaySession gatewaySession = setupConnection(
                channel,
                connectionId,
                sessionContext,
                sessionKey,
                libraryId,
                INITIATOR,
                closedResendInterval,
                resendRequestChunkSize,
                sendRedundantResendRequests,
                enableLastMsgSeqNumProcessed,
                FixDictionary.of(fixDictionary));
            library.addSession(gatewaySession);

            handoverNewConnectionToLibrary(
                libraryId,
                senderCompId,
                senderSubId,
                senderLocationId,
                targetCompId,
                targetSubId,
                targetLocationId,
                closedResendInterval,
                resendRequestChunkSize,
                sendRedundantResendRequests,
                enableLastMsgSeqNumProcessed,
                username,
                password,
                fixDictionary,
                heartbeatIntervalInS,
                correlationId,
                library,
                sessionContext,
                sessionKey,
                connectionId,
                sessionId,
                gatewaySession,
                header.sessionId(),
                header.position(),
                address.toString(),
                INITIATOR);
        }
        catch (final Exception e)
        {
            saveError(EXCEPTION, libraryId, correlationId, e);
        }
    }

    // Used when handing over a new connection that has never been gateway managed.
    // Eg: accepted sessions in initialAcceptedSessionOwner=SOLE_LIBRARY and also initiated sessions
    private void handoverNewConnectionToLibrary(
        final int libraryId,
        final String senderCompId,
        final String senderSubId,
        final String senderLocationId,
        final String targetCompId,
        final String targetSubId,
        final String targetLocationId,
        final boolean closedResendInterval,
        final int resendRequestChunkSize,
        final boolean sendRedundantResendRequests,
        final boolean enableLastMsgSeqNumProcessed,
        final String username,
        final String password,
        final Class<? extends FixDictionary> fixDictionary,
        final int heartbeatIntervalInS,
        final long correlationId,
        final LiveLibraryInfo library,
        final SessionContext sessionContext,
        final CompositeKey sessionKey,
        final long connectionId,
        final long sessionId,
        final GatewaySession gatewaySession,
        final int aeronSessionId,
        final long requiredPosition,
        final String address,
        final ConnectionType connectionType)
    {
        retryManager.schedule(new HandoverNewConnectionToLibrary(
            gatewaySession,
            aeronSessionId,
            requiredPosition,
            sessionId,
            connectionType,
            sessionContext,
            sessionKey,
            username,
            password,
            heartbeatIntervalInS,
            libraryId,
            connectionId,
            closedResendInterval,
            resendRequestChunkSize,
            sendRedundantResendRequests,
            enableLastMsgSeqNumProcessed,
            correlationId,
            senderCompId,
            senderSubId,
            senderLocationId,
            targetCompId,
            targetSubId,
            targetLocationId,
            address,
            fixDictionary,
            library));
    }

    private void saveError(final GatewayError error, final int libraryId, final long replyToId, final String message)
    {
        schedule(() -> inboundPublication.saveError(error, libraryId, replyToId, message));
    }

    private void saveError(final GatewayError error, final int libraryId, final long replyToId, final Exception e)
    {
        final String message = e.getMessage();
        saveError(error, libraryId, replyToId, message == null ? "" : message);
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
        final long position)
    {
        final long now = outboundTimer.recordSince(timestamp);

        senderEndPoints.onMessage(libraryId, connectionId, buffer, offset, length, sequenceNumber, position);

        if (nonLoggingPositionSender != null)
        {
            nonLoggingPositionSender.newPosition(libraryId, position);
        }

        sendTimer.recordSince(now);

        return CONTINUE;
    }

    private GatewaySession setupConnection(
        final TcpChannel channel,
        final long connectionId,
        final SessionContext context,
        final CompositeKey sessionKey,
        final int libraryId,
        final ConnectionType connectionType,
        final boolean closedResendInterval,
        final int resendRequestChunkSize,
        final boolean sendRedundantResendRequests,
        final boolean enableLastMsgSeqNumProcessed,
        final FixDictionary fixDictionary)
    {
        final ReceiverEndPoint receiverEndPoint = endPointFactory.receiverEndPoint(
            channel,
            connectionId,
            context.sessionId(),
            context.sequenceIndex(),
            libraryId,
            this);
        receiverEndPoints.add(receiverEndPoint);

        final BlockablePosition libraryBlockablePosition = getLibraryBlockablePosition(libraryId);
        final SenderEndPoint senderEndPoint = endPointFactory.senderEndPoint(
            channel, connectionId, libraryId, libraryBlockablePosition, this);
        senderEndPoints.add(senderEndPoint);

        final GatewaySession gatewaySession = new GatewaySession(
            connectionId,
            context,
            channel.remoteAddress(),
            connectionType,
            sessionKey,
            receiverEndPoint,
            senderEndPoint,
            this.onSessionlogon,
            closedResendInterval,
            resendRequestChunkSize,
            sendRedundantResendRequests,
            enableLastMsgSeqNumProcessed,
            fixDictionary,
            configuration.authenticationTimeoutInMs());

        receiverEndPoint.gatewaySession(gatewaySession);

        return gatewaySession;
    }

    private BlockablePosition getLibraryBlockablePosition(final int libraryId)
    {
        if (libraryId == ENGINE_LIBRARY_ID)
        {
            return engineBlockablePosition;
        }
        else
        {
            return idToLibrary.get(libraryId).librarySlowPeeker();
        }
    }

    public Action onRequestDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        return onDisconnect(libraryId, connectionId, reason);
    }

    public Action onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        receiverEndPoints.removeConnection(connectionId, reason);
        senderEndPoints.removeConnection(connectionId);
        final LiveLibraryInfo library = idToLibrary.get(libraryId);
        if (library != null)
        {
            library.removeSession(connectionId);
        }
        else
        {
            gatewaySessions.releaseByConnectionId(connectionId);
        }

        return CONTINUE;
    }

    public Action onLibraryConnect(
        final int libraryId,
        final String libraryName,
        final long correlationId,
        final int aeronSessionId)
    {
        final Action action = retryManager.retry(correlationId);
        if (action != null)
        {
            return action;
        }

        if (performingCloseOperation)
        {
            // Do not do allow a new library to connect whilst performing end of day operations.
            return CONTINUE;
        }

        final LiveLibraryInfo existingLibrary = idToLibrary.get(libraryId);
        if (existingLibrary != null)
        {
            existingLibrary.onHeartbeat(epochClock.time());

            return Pressure.apply(inboundPublication.saveControlNotification(libraryId, existingLibrary.sessions()));
        }

        if (soleLibraryMode && idToLibrary.size() >= 1)
        {
            logSoleLibraryError();
        }

        // Send an empty control notification if you've never seen this library before
        // Since it may have connected to another gateway node if you're clustered.
        if (Pressure.isBackPressured(
            inboundPublication.saveControlNotification(libraryId, Collections.emptyList())))
        {
            return ABORT;
        }

        final LivenessDetector livenessDetector = LivenessDetector.forEngine(
            inboundPublication,
            libraryId,
            configuration.replyTimeoutInMs(),
            epochClock.time());

        final List<Continuation> unitsOfWork = new ArrayList<>();
        unitsOfWork.add(() ->
        {
            final LibrarySlowPeeker librarySlowPeeker = this.librarySlowPeeker.addLibrary(aeronSessionId);
            if (librarySlowPeeker == null)
            {
                return BACK_PRESSURED;
            }

            final LiveLibraryInfo library = new LiveLibraryInfo(
                libraryId, libraryName, livenessDetector, aeronSessionId, librarySlowPeeker);
            idToLibrary.put(libraryId, library);

            DebugLogger.log(LIBRARY_MANAGEMENT, "Library %s - %s connected %n", libraryId, libraryName);

            return COMPLETE;
        });

        for (final GatewaySession gatewaySession : gatewaySessions.sessions())
        {
            unitsOfWork.add(
                () ->
                {
                    final InternalSession session = gatewaySession.session();

                    return saveManageSession(
                        libraryId,
                        gatewaySession,
                        session.lastSentMsgSeqNum(),
                        session.lastReceivedMsgSeqNum(),
                        LIBRARY_NOTIFICATION);
                });
        }

        return retryManager.firstAttempt(correlationId, new UnitOfWork(unitsOfWork));
    }

    public Action onApplicationHeartbeat(final int libraryId, final int aeronSessionId)
    {
        final LiveLibraryInfo library = idToLibrary.get(libraryId);
        if (library != null)
        {
            final long timeInMs = epochClock.time();
            DebugLogger.log(
                APPLICATION_HEARTBEAT, "Received Heartbeat from library %d at timeInMs %d%n", libraryId, timeInMs);
            library.onHeartbeat(timeInMs);

            return null;
        }

        // Don't skip messages from the engine managed library.
        if (libraryId == ENGINE_LIBRARY_ID)
        {
            return null;
        }

        return CONTINUE;
    }

    public Action onReleaseSession(
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final long correlationId,
        final SessionState state,
        final boolean awaitingResend,
        final long heartbeatIntervalInMs,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final String username,
        final String password,
        final Header header)
    {
        final LiveLibraryInfo libraryInfo = idToLibrary.get(libraryId);
        if (libraryInfo == null)
        {
            return Pressure.apply(inboundPublication.saveReleaseSessionReply(
                libraryId, SessionReplyStatus.UNKNOWN_LIBRARY, correlationId));
        }

        DebugLogger.log(
            LIBRARY_MANAGEMENT,
            "Releasing session %s with connectionId %s from library %s%n",
            sessionId,
            connectionId,
            libraryId);

        final GatewaySession session = libraryInfo.removeSession(connectionId);

        if (session == null)
        {
            return Pressure.apply(inboundPublication.saveReleaseSessionReply(
                libraryId, SessionReplyStatus.UNKNOWN_SESSION, correlationId));
        }

        final Action action = Pressure.apply(inboundPublication.saveReleaseSessionReply(libraryId, OK, correlationId));
        if (action == ABORT)
        {
            libraryInfo.addSession(session);
        }
        else
        {
            gatewaySessions.acquire(
                session,
                state,
                awaitingResend,
                (int)MILLISECONDS.toSeconds(heartbeatIntervalInMs),
                lastSentSequenceNumber,
                lastReceivedSequenceNumber,
                username,
                password,
                engineBlockablePosition);

            schedule(() -> saveManageSession(
                ENGINE_LIBRARY_ID,
                session,
                lastSentSequenceNumber,
                lastReceivedSequenceNumber,
                SessionStatus.LIBRARY_NOTIFICATION));
        }

        return action;
    }

    public Action onRequestSession(
        final int libraryId,
        final long sessionId,
        final long correlationId,
        final int replayFromSequenceNumber,
        final int replayFromSequenceIndex)
    {
        final Action action = retryManager.retry(correlationId);
        if (action != null)
        {
            return action;
        }

        final int aeronSessionId = outboundPublication.id();
        final long requiredPosition = outboundPublication.position();

        final LiveLibraryInfo libraryInfo = idToLibrary.get(libraryId);
        if (libraryInfo == null)
        {
            return Pressure.apply(inboundPublication.saveRequestSessionReply(
                libraryId, SessionReplyStatus.UNKNOWN_LIBRARY, correlationId));
        }

        final GatewaySession gatewaySession = gatewaySessions.releaseBySessionId(sessionId);
        if (gatewaySession == null)
        {
            return Pressure.apply(inboundPublication.saveRequestSessionReply(
                libraryId, SessionReplyStatus.UNKNOWN_SESSION, correlationId));
        }

        final InternalSession session = gatewaySession.session();
        if (!session.isActive())
        {
            return Pressure.apply(inboundPublication.saveRequestSessionReply(
                libraryId, SESSION_NOT_LOGGED_IN, correlationId));
        }

        final long connectionId = gatewaySession.connectionId();
        final int lastSentSeqNum = session.lastSentMsgSeqNum();
        final int lastRecvSeqNum = session.lastReceivedMsgSeqNum();

        gatewaySession.handoverManagementTo(libraryId, libraryInfo.librarySlowPeeker());
        libraryInfo.addSession(gatewaySession);

        DebugLogger.log(LIBRARY_MANAGEMENT, "Handing control for session %s to library %s%n", sessionId, libraryId);




        // Ensure that we've indexed up to this point in time.
        // If we don't do this then the indexer thread could receive a message sent from the Framer after
        // the library has sent its first message and get the wrong sent sequence number.
        // Only applies if there's a position to wait for and if the indexer is actually running on those messages.
        if (requiredPosition > 0 && configuration.logOutboundMessages())
        {
            return retryManager.firstAttempt(correlationId, () ->
            {
                if (sentIndexedPosition(aeronSessionId, requiredPosition))
                {
                    finishSessionHandover(
                        libraryId,
                        correlationId,
                        replayFromSequenceNumber,
                        replayFromSequenceIndex,
                        gatewaySession,
                        session,
                        connectionId,
                        lastSentSeqNum,
                        lastRecvSeqNum);

                    return COMPLETE;
                }
                else
                {
                    return BACK_PRESSURED;
                }
            });
        }
        else
        {
            finishSessionHandover(
                libraryId,
                correlationId,
                replayFromSequenceNumber,
                replayFromSequenceIndex,
                gatewaySession,
                session,
                connectionId,
                lastSentSeqNum,
                lastRecvSeqNum);

            return CONTINUE;
        }
    }

    private void finishSessionHandover(
        final int libraryId,
        final long correlationId,
        final int replayFromSequenceNumber,
        final int replayFromSequenceIndex,
        final GatewaySession gatewaySession,
        final InternalSession session,
        final long connectionId,
        final int lastSentSeqNum,
        final int lastRecvSeqNum)
    {
        final List<Continuation> continuations = new ArrayList<>();
        continuations.add(() -> saveManageSession(
            libraryId,
            gatewaySession,
            lastSentSeqNum,
            lastRecvSeqNum,
            SessionStatus.SESSION_HANDOVER,
            session.compositeKey(),
            connectionId,
            session,
            correlationId));

        catchupSession(
            continuations,
            libraryId,
            connectionId,
            correlationId,
            replayFromSequenceNumber,
            replayFromSequenceIndex,
            gatewaySession,
            lastRecvSeqNum);

        retryManager.schedule(new UnitOfWork(continuations));
    }

    public Action onFollowerSessionRequest(
        final int libraryId,
        final long correlationId,
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength)
    {
        asciiBuffer.wrap(srcBuffer);
        acceptorHeaderDecoder.decode(asciiBuffer, srcOffset, srcLength);

        final CompositeKey compositeKey = sessionIdStrategy.onAcceptLogon(acceptorHeaderDecoder);
        final SessionContext sessionContext = sessionContexts.newSessionContext(compositeKey);
        final long sessionId = sessionContext.sessionId();

        retryManager.schedule(() -> inboundPublication.saveFollowerSessionReply(
            libraryId,
            correlationId,
            sessionId));

        return CONTINUE;
    }

    private long saveManageSession(
        final int libraryId,
        final GatewaySession gatewaySession,
        final int lastSentSeqNum,
        final int lastReceivedSeqNum,
        final SessionStatus logonstatus)
    {
        final CompositeKey compositeKey = gatewaySession.sessionKey();
        if (compositeKey != null)
        {
            final long connectionId = gatewaySession.connectionId();

            final InternalSession session = gatewaySession.session();
            return saveManageSession(
                libraryId,
                gatewaySession,
                lastSentSeqNum,
                lastReceivedSeqNum,
                logonstatus,
                compositeKey,
                connectionId,
                session,
                NO_CORRELATION_ID);
        }

        return COMPLETE;
    }

    private long saveManageSession(
        final int libraryId,
        final GatewaySession gatewaySession,
        final int lastSentSeqNum,
        final int lastReceivedSeqNum,
        final SessionStatus sessionstatus,
        final CompositeKey compositeKey,
        final long connectionId,
        final InternalSession session,
        final long correlationId)
    {
        return inboundPublication.saveManageSession(
            libraryId,
            connectionId,
            gatewaySession.sessionId(),
            lastSentSeqNum,
            lastReceivedSeqNum,
            session.logonTime(),
            sessionstatus,
            gatewaySession.slowStatus(),
            gatewaySession.connectionType(),
            session.state(),
            session.awaitingResend(),
            gatewaySession.heartbeatIntervalInS(),
            gatewaySession.closedResendInterval(),
            gatewaySession.resendRequestChunkSize(),
            gatewaySession.sendRedundantResendRequests(),
            gatewaySession.enableLastMsgSeqNumProcessed(),
            correlationId,
            gatewaySession.sequenceIndex(),
            session.lastResentMsgSeqNo(),
            session.lastResendChunkMsgSeqNum(),
            session.endOfResendRequestRange(),
            session.awaitingHeartbeat(),
            gatewaySession.logonReceivedSequenceNumber(),
            gatewaySession.logonSequenceIndex(),
            compositeKey.localCompId(),
            compositeKey.localSubId(),
            compositeKey.localLocationId(),
            compositeKey.remoteCompId(),
            compositeKey.remoteSubId(),
            compositeKey.remoteLocationId(),
            gatewaySession.address(),
            gatewaySession.username(),
            gatewaySession.password(),
            gatewaySession.fixDictionary().getClass());
    }

    private void catchupSession(
        final List<Continuation> continuations,
        final int libraryId,
        final long connectionId,
        final long correlationId,
        final int replayFromSequenceNumber,
        final int requestedReplayFromSequenceIndex,
        final GatewaySession session,
        final int lastReceivedSeqNum)
    {
        if (replayFromSequenceNumber != NO_MESSAGE_REPLAY)
        {
            if (!configuration.logInboundMessages())
            {
                continuations.add(() ->
                {
                    final long position = inboundPublication.saveRequestSessionReply(
                        libraryId, INVALID_CONFIGURATION_NOT_LOGGING_MESSAGES, correlationId);
                    if (position > 0)
                    {
                        session.play();
                    }
                    return position;
                });
                return;
            }

            final int replayFromSequenceIndex;
            final int sequenceIndex = session.sequenceIndex();
            if (requestedReplayFromSequenceIndex == CURRENT_SEQUENCE)
            {
                replayFromSequenceIndex = sequenceIndex;
            }
            else
            {
                if (requestedReplayFromSequenceIndex > sequenceIndex ||
                    (requestedReplayFromSequenceIndex == sequenceIndex &&
                    replayFromSequenceNumber > lastReceivedSeqNum))
                {
                    continuations.add(() -> sequenceNumberTooHigh(libraryId, correlationId, session));
                    return;
                }
                replayFromSequenceIndex = requestedReplayFromSequenceIndex;
            }

            continuations.add(new CatchupReplayer(
                receivedSequenceNumberIndex,
                inboundMessages,
                inboundPublication,
                errorHandler,
                correlationId,
                connectionId,
                libraryId,
                lastReceivedSeqNum,
                sequenceIndex,
                replayFromSequenceNumber,
                replayFromSequenceIndex,
                session,
                catchupTimeout(),
                epochClock));
        }
        else
        {
            continuations.add(() -> CatchupReplayer.sendOk(inboundPublication, correlationId, session, libraryId));
        }
    }

    private long catchupTimeout()
    {
        return configuration.replyTimeoutInMs() / 2;
    }

    private long sequenceNumberTooHigh(final int libraryId, final long correlationId, final GatewaySession session)
    {
        final long position = inboundPublication.saveRequestSessionReply(
            libraryId, SEQUENCE_NUMBER_TOO_HIGH, correlationId);
        if (!Pressure.isBackPressured(position))
        {
            session.play();
        }
        return position;
    }

    private void onSessionLogon(final GatewaySession gatewaySession)
    {
        if (!soleLibraryMode)
        {
            // Notify libraries of the existence of this logged on session.
            schedule(() ->
            {
                final InternalSession session = gatewaySession.session();
                if (null == session)
                {
                    // Another library is now handling the session, don't publish availability.
                    return COMPLETE;
                }

                final CompositeKey key = gatewaySession.sessionKey();
                return saveManageSession(
                    ENGINE_LIBRARY_ID,
                    gatewaySession,
                    session.lastSentMsgSeqNum(),
                    session.lastReceivedMsgSeqNum(),
                    SessionStatus.SESSION_HANDOVER,
                    key,
                    gatewaySession.connectionId(),
                    session,
                    NO_CORRELATION_ID);
            });
        }
    }

    void onLogonMessageReceived(final GatewaySession gatewaySession)
    {
        // Hand over management of this new session to the sole library
        if (soleLibraryMode && gatewaySession.connectionType() == ACCEPTOR)
        {
            if (idToLibrary.size() != 1)
            {
                logSoleLibraryError();
            }

            final LiveLibraryInfo libraryInfo = idToLibrary.values().iterator().next();
            final CompositeKey sessionKey = gatewaySession.sessionKey();
            final int libraryAeronSessionId = libraryInfo.aeronSessionId();
            final long requiredPosition = librarySubscription.imageBySessionId(libraryAeronSessionId).position();

            final int libraryId = libraryInfo.libraryId();
            gatewaySession.setManagementTo(libraryId, libraryInfo.librarySlowPeeker());
            libraryInfo.addSession(gatewaySession);

            handoverNewConnectionToLibrary(
                libraryId,
                sessionKey.localCompId(),
                sessionKey.localSubId(),
                sessionKey.localLocationId(),
                sessionKey.remoteCompId(),
                sessionKey.remoteSubId(),
                sessionKey.remoteLocationId(),
                gatewaySession.closedResendInterval(),
                gatewaySession.resendRequestChunkSize(),
                gatewaySession.sendRedundantResendRequests(),
                gatewaySession.enableLastMsgSeqNumProcessed(),
                gatewaySession.username(),
                gatewaySession.password(),
                gatewaySession.fixDictionary().getClass(),
                gatewaySession.heartbeatIntervalInS(),
                NO_CORRELATION_ID,
                libraryInfo,
                gatewaySession.context(),
                sessionKey,
                gatewaySession.connectionId(),
                gatewaySession.sessionId(),
                gatewaySession,
                libraryAeronSessionId,
                requiredPosition,
                gatewaySession.address(),
                ACCEPTOR);
        }
    }

    private void logSoleLibraryError()
    {
        errorHandler.onError(new IllegalStateException(
            "Error, invalid numbers of libraryies: " + idToLibrary.size() + " whilst in sole library mode"));
    }

    void onQueryLibraries(final QueryLibrariesCommand command)
    {
        final List<LibraryInfo> libraries = new ArrayList<>(idToLibrary.values());
        libraries.add(new EngineLibraryInfo(gatewaySessions));
        command.success(libraries);
    }

    void onResetSessionIds(final File backupLocation, final ResetSessionIdsCommand command)
    {
        schedule(new UnitOfWork(
            inboundPublication::saveResetSessionIds,
            outboundPublication::saveResetSessionIds,
            () ->
            {
                try
                {
                    sessionContexts.reset(backupLocation);
                }
                catch (final Exception ex)
                {
                    command.onError(ex);
                }

                return COMPLETE;
            },
            () ->
            {
                if (command.hasCompleted())
                {
                    return COMPLETE;
                }

                if (sequenceNumbersNotReset())
                {
                    return BACK_PRESSURED;
                }
                else
                {
                    command.success();
                    return COMPLETE;
                }
            }));
    }

    void onStartClose(final StartCloseCommand startCloseCommand)
    {
        performingCloseOperation = true;

        schedule(new CloseOperation(
            inboundPublication,
            new ArrayList<>(idToLibrary.values()),
            // Take a copy to avoid library sessions being acquired causing issues
            new ArrayList<>(gatewaySessions.sessions()),
            receiverEndPoints,
            startCloseCommand));
    }

    void onResetSequenceNumber(final ResetSequenceNumberCommand reply)
    {
        reply.libraryLookup(senderEndPoints.libraryLookup());

        if (!reply.poll())
        {
            replies.add(reply);
        }
    }

    void onLookupSessionId(final LookupSessionIdCommand command)
    {
        final CompositeKey compositeKey = sessionIdStrategy.onInitiateLogon(
            command.localCompId,
            command.localSubId,
            command.localLocationId,
            command.remoteCompId,
            command.remoteSubId,
            command.remoteLocationId);

        final long sessionId = sessionContexts.lookupSessionId(compositeKey);

        if (sessionId == Session.UNKNOWN)
        {
            command.error(new IllegalArgumentException("Unknown Session: " + compositeKey));
        }
        else
        {
            command.complete(sessionId);
        }
    }

    private boolean sequenceNumbersNotReset()
    {
        return sentSequenceNumberIndex.lastKnownSequenceNumber(1) != UNK_SESSION ||
            receivedSequenceNumberIndex.lastKnownSequenceNumber(1) != UNK_SESSION;
    }

    public void onClose()
    {
        if (configuration.gracefulShutdown())
        {
            closeAll(
                this::quiesce,
                retryManager,
                inboundMessages,
                receiverEndPoints,
                senderEndPoints,
                channelSupplier);
        }
        else
        {
            closeAll(
                inboundMessages,
                channelSupplier);
        }
    }

    private void quiesce()
    {
        final Long2LongHashMap inboundPositions = new Long2LongHashMap(CompletionPosition.MISSING_VALUE);
        inboundPositions.put(inboundPublication.id(), inboundPublication.position());
        inboundCompletionPosition.complete(inboundPositions);

        final Long2LongHashMap outboundPositions = new Long2LongHashMap(CompletionPosition.MISSING_VALUE);
        idToLibrary.values().forEach(liveLibraryInfo ->
        {
            final int aeronSessionId = liveLibraryInfo.aeronSessionId();
            final Image image = librarySubscription.imageBySessionId(aeronSessionId);
            if (image != null)
            {
                final long position = image.position();
                outboundPositions.put(aeronSessionId, position);
            }
        });

        outboundLibraryCompletionPosition.complete(outboundPositions);

        recordingCoordinator.completionPositions(inboundPositions, outboundPositions);
    }

    public String roleName()
    {
        return agentNamePrefix + "Framer";
    }

    void schedule(final Continuation continuation)
    {
        if (continuation.attemptToAction() != CONTINUE)
        {
            retryManager.schedule(continuation);
        }
    }

    void slowStatus(final int libraryId, final long connectionId, final boolean hasBecomeSlow)
    {
        if (hasBecomeSlow)
        {
            sendSlowStatus(libraryId, connectionId, resendNotSlowStatus, resendSlowStatus, SlowStatus.SLOW);
        }
        else
        {
            sendSlowStatus(libraryId, connectionId, resendSlowStatus, resendNotSlowStatus, SlowStatus.NOT_SLOW);
        }
    }

    private void sendSlowStatus(
        final int libraryId,
        final long connectionId,
        final Long2LongHashMap toNotResend,
        final Long2LongHashMap toResend,
        final SlowStatus status)
    {
        toNotResend.remove(connectionId);
        final long position = inboundPublication.saveSlowStatusNotification(libraryId, connectionId, status);

        if (Pressure.isBackPressured(position))
        {
            toResend.put(connectionId, libraryId);
        }
    }

    void receiverEndPointPollingOptional(final long connectionId)
    {
        receiverEndPoints.receiverEndPointPollingOptional(connectionId);
    }

    void onBind(final BindCommand bindCommand)
    {
        try
        {
            if (bindCommand.bind())
            {
                channelSupplier.bind();
            }
            else
            {
                channelSupplier.unbind();
            }
            bindCommand.success();
        }
        catch (final Exception e)
        {
            bindCommand.onError(e);
        }
    }

    class HandoverNewConnectionToLibrary extends UnitOfWork
    {
        private final GatewaySession gatewaySession;
        private final int aeronSessionId;
        private final long requiredPosition;
        private final long sessionId;
        private final ConnectionType connectionType;
        private final SessionContext sessionContext;
        private final CompositeKey sessionKey;
        private final String username;
        private final String password;
        private final int heartbeatIntervalInS;
        private final int libraryId;
        private final long connectionId;
        private final boolean closedResendInterval;
        private final int resendRequestChunkSize;
        private final boolean sendRedundantResendRequests;
        private final boolean enableLastMsgSeqNumProcessed;
        private final long correlationId;
        private final String senderCompId;
        private final String senderSubId;
        private final String senderLocationId;
        private final String targetCompId;
        private final String targetSubId;
        private final String targetLocationId;
        private final String address;
        private final Class<? extends FixDictionary> fixDictionary;
        private final LiveLibraryInfo library;
        private int lastSentSequenceNumber;
        private int lastReceivedSequenceNumber;

        HandoverNewConnectionToLibrary(
            final GatewaySession gatewaySession,
            final int aeronSessionId,
            final long requiredPosition,
            final long sessionId,
            final ConnectionType connectionType,
            final SessionContext sessionContext,
            final CompositeKey sessionKey,
            final String username,
            final String password,
            final int heartbeatIntervalInS,
            final int libraryId,
            final long connectionId,
            final boolean closedResendInterval,
            final int resendRequestChunkSize,
            final boolean sendRedundantResendRequests,
            final boolean enableLastMsgSeqNumProcessed,
            final long correlationId,
            final String senderCompId,
            final String senderSubId,
            final String senderLocationId,
            final String targetCompId,
            final String targetSubId,
            final String targetLocationId,
            final String address,
            final Class<? extends FixDictionary> fixDictionary,
            final LiveLibraryInfo library)
        {
            this.gatewaySession = gatewaySession;
            this.aeronSessionId = aeronSessionId;
            this.requiredPosition = requiredPosition;
            this.sessionId = sessionId;
            this.connectionType = connectionType;
            this.sessionContext = sessionContext;
            this.sessionKey = sessionKey;
            this.username = username;
            this.password = password;
            this.heartbeatIntervalInS = heartbeatIntervalInS;
            this.libraryId = libraryId;
            this.connectionId = connectionId;
            this.closedResendInterval = closedResendInterval;
            this.resendRequestChunkSize = resendRequestChunkSize;
            this.sendRedundantResendRequests = sendRedundantResendRequests;
            this.enableLastMsgSeqNumProcessed = enableLastMsgSeqNumProcessed;
            this.correlationId = correlationId;
            this.senderCompId = senderCompId;
            this.senderSubId = senderSubId;
            this.senderLocationId = senderLocationId;
            this.targetCompId = targetCompId;
            this.targetSubId = targetSubId;
            this.targetLocationId = targetLocationId;
            this.address = address;
            this.fixDictionary = fixDictionary;
            this.library = library;

            if (configuration.logInboundMessages())
            {
                work(this::checkLoggerUpToDate, this::saveManageSession);
            }
            else
            {
                work(this::onLogon, this::saveManageSession);
            }
        }

        private long checkLoggerUpToDate()
        {
            if (gatewaySession.initialResetSeqNum())
            {
                lastSentSequenceNumber = 0;
                lastReceivedSequenceNumber = 0;
                return 0;
            }

            if (sentIndexedPosition(aeronSessionId, requiredPosition))
            {
                lastSentSequenceNumber = sentSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
                lastReceivedSequenceNumber = receivedSequenceNumberIndex.lastKnownSequenceNumber(sessionId);

                // Accptors are adjusted here - symmetrically with the non initialAcceptedSessionOwner=SOLE_LIBRARY
                // case, whilst Initiator configuration is always adjusted on the library side.
                if (connectionType == ACCEPTOR)
                {
                    lastSentSequenceNumber = adjustLastSequenceNumber(lastSentSequenceNumber);
                    lastReceivedSequenceNumber = adjustLastSequenceNumber(lastReceivedSequenceNumber);
                }

                return onLogon();
            }

            return BACK_PRESSURED;
        }

        private long onLogon()
        {
            gatewaySession.onLogon(
                sessionId,
                sessionContext,
                sessionKey,
                username,
                password,
                heartbeatIntervalInS,
                lastReceivedSequenceNumber);

            return COMPLETE;
        }

        private long saveManageSession()
        {
            final long position = inboundPublication.saveManageSession(
                libraryId,
                connectionId,
                sessionId,
                lastSentSequenceNumber,
                lastReceivedSequenceNumber,
                Session.NO_LOGON_TIME,
                SessionStatus.SESSION_HANDOVER,
                SlowStatus.NOT_SLOW,
                connectionType,
                CONNECTED,
                false,
                heartbeatIntervalInS,
                closedResendInterval,
                resendRequestChunkSize,
                sendRedundantResendRequests,
                enableLastMsgSeqNumProcessed,
                correlationId,
                sessionContext.sequenceIndex(),
                InternalSession.INITIAL_LAST_RESENT_MSG_SEQ_NO,
                InternalSession.INITIAL_LAST_RESEND_CHUNK_MSG_SEQ_NUM,
                InternalSession.INITIAL_END_OF_RESEND_REQUEST_RANGE,
                InternalSession.INITIAL_AWAITING_HEARTBEAT,
                gatewaySession.logonReceivedSequenceNumber(),
                gatewaySession.logonSequenceIndex(),
                senderCompId,
                senderSubId,
                senderLocationId,
                targetCompId,
                targetSubId,
                targetLocationId,
                address,
                username,
                password,
                fixDictionary);

            if (position > 0)
            {
                library.connectionFinishesConnecting(correlationId);
                gatewaySession.play();
            }

            return position;
        }
    }
}
