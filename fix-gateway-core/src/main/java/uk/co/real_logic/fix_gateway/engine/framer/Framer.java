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
package uk.co.real_logic.fix_gateway.engine.framer;

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
import org.agrona.collections.Long2LongHashMap.LongIterator;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.QueuedPipe;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.LivenessDetector;
import uk.co.real_logic.fix_gateway.Pressure;
import uk.co.real_logic.fix_gateway.dictionary.generation.Exceptions;
import uk.co.real_logic.fix_gateway.engine.CompletionPosition;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.EngineDescriptorStore;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.framer.SubscriptionSlowPeeker.LibrarySlowPeeker;
import uk.co.real_logic.fix_gateway.engine.framer.TcpChannelSupplier.NewChannelHandler;
import uk.co.real_logic.fix_gateway.engine.logger.ReplayQuery;
import uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.protocol.*;
import uk.co.real_logic.fix_gateway.replication.ClusterFragmentAssembler;
import uk.co.real_logic.fix_gateway.replication.ClusterFragmentHandler;
import uk.co.real_logic.fix_gateway.replication.ClusterSubscription;
import uk.co.real_logic.fix_gateway.replication.ClusterableStreams;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.timing.Timer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.agrona.collections.CollectionUtil.removeIf;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.LogTag.APPLICATION_HEARTBEAT;
import static uk.co.real_logic.fix_gateway.LogTag.CLUSTER_MANAGEMENT;
import static uk.co.real_logic.fix_gateway.Pressure.isBackPressured;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.engine.SessionInfo.UNK_SESSION;
import static uk.co.real_logic.fix_gateway.engine.framer.Continuation.COMPLETE;
import static uk.co.real_logic.fix_gateway.engine.framer.SessionContexts.UNKNOWN_SESSION;
import static uk.co.real_logic.fix_gateway.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.ACCEPTOR;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.fix_gateway.messages.GatewayError.*;
import static uk.co.real_logic.fix_gateway.messages.LogonStatus.LIBRARY_NOTIFICATION;
import static uk.co.real_logic.fix_gateway.messages.SequenceNumberType.DETERMINE_AT_LOGON;
import static uk.co.real_logic.fix_gateway.messages.SequenceNumberType.TRANSIENT;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.*;
import static uk.co.real_logic.fix_gateway.messages.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.messages.SessionState.CONNECTED;

/**
 * Handles incoming connections from clients and outgoing connections to exchanges.
 */
class Framer implements Agent, EngineEndPointHandler, ProtocolHandler
{
    // TODO(Nick): This is a copy of the one from LibraryPoller,
    // TODO(Nick): This is a copy of the one from LibraryPoller,
    // but that one is also private so we can't access. Combine somewhere.
    // Combine somewhere.
    static final long NO_CORRELATION_ID = 0;
    private static final ByteBuffer CONNECT_ERROR;
    private static final List<SessionInfo> NO_SESSIONS = emptyList();

    static
    {
        final byte[] errorBytes =
            "This is not the cluster's leader node, please connect to the leader".getBytes(US_ASCII);
        CONNECT_ERROR = ByteBuffer.wrap(errorBytes);
    }

    private final RetryManager retryManager = new RetryManager();
    private final List<ResetSequenceNumberCommand> replies = new ArrayList<>();
    private final Int2ObjectHashMap<LiveLibraryInfo> idToLibrary = new Int2ObjectHashMap<>();
    private final List<LiveLibraryInfo> librariesBeingAcquired = new ArrayList<>();
    private final Consumer<AdminCommand> onAdminCommand = command -> command.execute(this);
    private final NewChannelHandler onNewConnectionFunc = this::onNewConnection;
    private final Predicate<LiveLibraryInfo> retryAcquireLibrarySessionsFunc = this::retryAcquireLibrarySessions;

    private final TcpChannelSupplier channelSupplier;
    private final EpochClock clock;
    private final Timer outboundTimer;
    private final Timer sendTimer;

    private final ControlledFragmentHandler librarySubscriber;
    private final ControlledFragmentHandler replaySubscriber;
    private final ControlledFragmentHandler replaySlowSubscriber;
    private final ClusterFragmentHandler clusterSubscriber;

    private final ReceiverEndPoints receiverEndPoints = new ReceiverEndPoints();
    private final ControlledFragmentAssembler senderEndPointAssembler;
    private final SenderEndPoints senderEndPoints;

    private final EngineConfiguration configuration;
    private final EndPointFactory endPointFactory;
    private final ClusterableStreams clusterableStreams;
    private final ClusterSubscription clusterSubscription;
    private final ClusterSlowPeeker clusterSlowPeeker;
    private final Subscription librarySubscription;
    private final SubscriptionSlowPeeker librarySlowPeeker;
    private final Image replayImage;
    private final SlowPeeker replaySlowPeeker;
    private final BlockablePosition engineBlockablePosition;
    private final GatewayPublication inboundPublication;
    private final String agentNamePrefix;
    private final CompletionPosition inboundCompletionPosition;
    private final CompletionPosition outboundLibraryCompletionPosition;
    private final CompletionPosition outboundClusterCompletionPosition;
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
    /**
     * Null if inbound messages are not logged
     */
    private final ReplayQuery inboundMessages;
    private final ErrorHandler errorHandler;
    private final GatewayPublication outboundPublication;
    // Both connection id to library id maps
    private final Long2LongHashMap resendSlowStatus = new Long2LongHashMap(-1);
    private final Long2LongHashMap resendNotSlowStatus = new Long2LongHashMap(-1);

    private long nextConnectionId = (long)(Math.random() * Long.MAX_VALUE);

    Framer(
        final EpochClock clock,
        final Timer outboundTimer,
        final Timer sendTimer,
        final EngineConfiguration configuration,
        final EndPointFactory endPointFactory,
        final ClusterableStreams clusterableStreams,
        final ClusterSubscription clusterSubscription,
        final ClusterSubscription clusterSlowSubscription,
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
        final EngineDescriptorStore engineDescriptorStore,
        final LongHashSet replicatedConnectionIds,
        final String agentNamePrefix,
        final CompletionPosition inboundCompletionPosition,
        final CompletionPosition outboundLibraryCompletionPosition,
        final CompletionPosition outboundClusterCompletionPosition,
        final FinalImagePositions finalImagePositions)
    {
        this.clock = clock;
        this.outboundTimer = outboundTimer;
        this.sendTimer = sendTimer;
        this.configuration = configuration;
        this.endPointFactory = endPointFactory;
        this.clusterSubscription = clusterSubscription;
        this.librarySubscription = librarySubscription;
        this.replayImage = replayImage;
        this.gatewaySessions = gatewaySessions;
        this.inboundMessages = inboundMessages;
        this.errorHandler = errorHandler;
        this.outboundPublication = outboundPublication;
        this.inboundPublication = inboundPublication;
        this.clusterableStreams = clusterableStreams;
        this.agentNamePrefix = agentNamePrefix;
        this.inboundCompletionPosition = inboundCompletionPosition;
        this.outboundLibraryCompletionPosition = outboundLibraryCompletionPosition;
        this.outboundClusterCompletionPosition = outboundClusterCompletionPosition;
        this.senderEndPoints = new SenderEndPoints(errorHandler);
        this.senderEndPointAssembler = new ControlledFragmentAssembler(senderEndPoints);
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionContexts = sessionContexts;
        this.adminCommands = adminCommands;
        this.sentSequenceNumberIndex = sentSequenceNumberIndex;
        this.receivedSequenceNumberIndex = receivedSequenceNumberIndex;
        this.finalImagePositions = finalImagePositions;

        this.librarySlowPeeker = new SubscriptionSlowPeeker(slowSubscription, librarySubscription);

        this.outboundLibraryFragmentLimit = configuration.outboundLibraryFragmentLimit();
        this.replayFragmentLimit = configuration.replayFragmentLimit();
        this.inboundBytesReceivedLimit = configuration.inboundBytesReceivedLimit();

        this.replaySlowPeeker = new SlowPeeker(replaySlowImage, replayImage);
        endPointFactory.replaySlowPeeker(replaySlowPeeker);

        if (isClustered())
        {
            clusterSlowPeeker = new ClusterSlowPeeker(clusterSubscription, clusterSlowSubscription);
            engineBlockablePosition = clusterSlowPeeker;
            librarySubscriber = new ControlledFragmentAssembler(new SubscriptionSplitter(
                clusterableStreams,
                new EngineProtocolSubscription(this),
                clusterableStreams.publication(OUTBOUND_LIBRARY_STREAM, "outboundLibraryStream"),
                inboundPublication,
                engineDescriptorStore,
                configuration.bindAddress().toString(),
                replicatedConnectionIds));
            clusterSubscriber = new ClusterFragmentAssembler(ProtocolSubscription.of(this));
        }
        else
        {
            engineBlockablePosition = getOutboundSlowPeeker(outboundPublication);
            librarySubscriber = new ControlledFragmentAssembler(
                ProtocolSubscription.of(this, new EngineProtocolSubscription(this)));
            clusterSubscriber = null;
            clusterSlowPeeker = null;
        }

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
                final int messageType,
                final long timestamp,
                final MessageStatus status,
                final long position)
            {
                return senderEndPoints.onReplayMessage(connectionId, buffer, offset, length, position);
            }

            public Action onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
            {
                // Should never be replayed.
                return Action.CONTINUE;
            }
        }));

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
                final int messageType,
                final long timestamp,
                final MessageStatus status,
                final long position)
            {
                return senderEndPoints.onSlowReplayMessage(connectionId, buffer, offset, length, position);
            }

            public Action onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
            {
                // Should never be replayed.
                return Action.CONTINUE;
            }
        }));

        try
        {
            channelSupplier = configuration.channelSupplier();
        }
        catch (final IOException ex)
        {
            throw new IllegalArgumentException(ex);
        }
    }

    private LibrarySlowPeeker getOutboundSlowPeeker(final GatewayPublication outboundPublication)
    {
        final int outboundSessionId = outboundPublication.id();
        LibrarySlowPeeker outboundSlowPeeker;
        while ((outboundSlowPeeker = this.librarySlowPeeker.addLibrary(outboundSessionId)) == null)
        {
            configuration.invokeConductorAgent();

            Thread.yield();
        }

        return outboundSlowPeeker;
    }

    private boolean isClustered()
    {
        return clusterSubscription != null;
    }

    public int doWork() throws Exception
    {
        final long timeInMs = clock.time();
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
            final LongIterator keyIterator = resend.keySet().iterator();
            while (keyIterator.hasNext())
            {
                final long connectionId = keyIterator.nextValue();
                final int libraryId = (int) resend.get(connectionId);
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
        int messagesRead =
            librarySubscription.controlledPoll(librarySubscriber, outboundLibraryFragmentLimit);
        messagesRead +=
            librarySlowPeeker.peek(senderEndPointAssembler);

        if (isClustered())
        {
            messagesRead +=
                clusterSubscription.poll(clusterSubscriber, outboundLibraryFragmentLimit);
            messagesRead +=
                clusterSlowPeeker.peek(senderEndPoints);
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
                DebugLogger.log(CLUSTER_MANAGEMENT, "Timing out connection to library %s%n", library.libraryId());

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

        final boolean indexed = indexedPosition(librarySessionId, libraryPosition);
        if (indexed)
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
        final boolean indexed = indexedPosition(library.aeronSessionId(), library.acquireAtPosition());
        if (indexed)
        {
            acquireLibrarySessions(library);
        }

        return indexed;
    }

    private boolean indexedPosition(final int aeronSessionId, final long position)
    {
        return sentSequenceNumberIndex.indexedPosition(aeronSessionId) >= position;
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
                CLUSTER_MANAGEMENT,
                "Acquiring session %s from library %s%n", session.sessionId(), library.libraryId());

            gatewaySessions.acquire(
                session,
                state,
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
                LogonStatus.LIBRARY_NOTIFICATION));
        }

        finalImagePositions.removePosition(library.aeronSessionId());
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
        return channelSupplier.pollSelector(timeInMs, onNewConnectionFunc);
    }

    private void onNewConnection(final long timeInMs, final TcpChannel channel) throws IOException
    {
        if (clusterableStreams.isLeader())
        {
            final long connectionId = this.nextConnectionId++;
            final GatewaySession session = setupConnection(
                channel, connectionId, UNKNOWN_SESSION, null, ENGINE_LIBRARY_ID, ACCEPTOR, DETERMINE_AT_LOGON);

            session.disconnectAt(timeInMs + configuration.noLogonDisconnectTimeoutInMs());

            gatewaySessions.acquire(
                session,
                CONNECTED,
                configuration.defaultHeartbeatIntervalInS(),
                UNK_SESSION,
                UNK_SESSION,
                null,
                null,
                engineBlockablePosition);

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
        else
        {
            final String address = channel.remoteAddress();
            errorHandler.onError(new IllegalStateException(
                String.format("Attempted connection from %s whilst follower", address)));

            // NB: channel is still blocking at this point, so will be placed in buffer
            // NB: Writing error message is best effort, possible that other end closes
            // Before receipt.
            channel.write(CONNECT_ERROR);
            channel.close();
        }
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
        final int requestedInitialSequenceNumber,
        final boolean resetSequenceNumber,
        final String username,
        final String password,
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
            saveError(DUPLICATE_SESSION, libraryId, correlationId,
                      "Duplicate Session for: " + sessionKey +
                      " Surrogate Key: " + sessionId +
                      " Currently owned by " + owningLibraryId);

            return CONTINUE;
        }

        try
        {
            final InetSocketAddress address = new InetSocketAddress(host, port);
            channelSupplier.open(address,
                (channel, ex) ->
                {
                    if (ex != null)
                    {
                        sessionContexts.onDisconnect(sessionContext.sessionId());
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
                        username,
                        password,
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
        catch (final Exception e)
        {
            sessionContexts.onDisconnect(sessionContext.sessionId());
            saveError(UNABLE_TO_CONNECT, libraryId, correlationId, e);

            return CONTINUE;
        }

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
        final String username,
        final String password,
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
            DebugLogger.log(
                CLUSTER_MANAGEMENT,
                "Initiating session %s from library %s%n", sessionContext.sessionId(), library.libraryId());

            final long connectionId = this.nextConnectionId++;

            sessionContext.onLogon(resetSequenceNumber || sequenceNumberType == TRANSIENT);
            final long sessionId = sessionContext.sessionId();
            final GatewaySession session =
                setupConnection(
                    channel,
                    connectionId,
                    sessionContext,
                    sessionKey,
                    libraryId,
                    INITIATOR,
                    sequenceNumberType);

            library.addSession(session);

            final class FinishInitiatingConnection extends UnitOfWork
            {
                private int lastSentSequenceNumber;
                private int lastReceivedSequenceNumber;

                private FinishInitiatingConnection()
                {
                    work(
                        this::checkLoggerUpToDate,
                        this::saveManageSession);
                }

                private long saveManageSession()
                {
                    final long result = inboundPublication.saveManageSession(libraryId,
                                                                        connectionId,
                                                                        sessionId,
                                                                        lastSentSequenceNumber,
                                                                        lastReceivedSequenceNumber,
                                                                        Session.NO_LOGON_TIME,
                                                                        LogonStatus.NEW,
                                                                        SlowStatus.NOT_SLOW,
                                                                        INITIATOR,
                                                                        CONNECTED,
                                                                        heartbeatIntervalInS,
                                                                        correlationId,
                                                                        sessionContext.sequenceIndex(),
                                                                        senderCompId,
                                                                        senderSubId,
                                                                        senderLocationId,
                                                                        targetCompId,
                                                                        targetSubId,
                                                                        targetLocationId,
                                                                        address.toString());
                    return result;
                }

                private long checkLoggerUpToDate()
                {
                    if (indexedPosition(header.sessionId(), header.position()))
                    {
                        lastSentSequenceNumber = sentSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
                        lastReceivedSequenceNumber = receivedSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
                        session.onLogon(
                            sessionId, sessionContext, sessionKey, username, password, heartbeatIntervalInS);
                        return 0;
                    }

                    return BACK_PRESSURED;
                }
            }

            retryManager.schedule(new FinishInitiatingConnection());
        }
        catch (final Exception e)
        {
            saveError(EXCEPTION, libraryId, correlationId, e);
        }
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
        final int messageType,
        final long timestamp,
        final MessageStatus status,
        final long position)
    {
        final long now = outboundTimer.recordSince(timestamp);

        if (!clusterableStreams.isLeader())
        {
            sessionContexts.onSentFollowerMessage(sessionId, sequenceIndex, messageType, buffer, offset, length);
        }

        senderEndPoints.onMessage(libraryId, connectionId, buffer, offset, length, position);

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
        final SequenceNumberType sequenceNumberType)
        throws IOException
    {
        final ReceiverEndPoint receiverEndPoint =
            endPointFactory.receiverEndPoint(channel, connectionId, context.sessionId(), context.sequenceIndex(),
                libraryId, this,
                sentSequenceNumberIndex, receivedSequenceNumberIndex, sequenceNumberType, connectionType);
        receiverEndPoints.add(receiverEndPoint);

        final BlockablePosition libraryBlockablePosition = getLibraryBlockablePosition(libraryId);
        final SenderEndPoint senderEndPoint =
            endPointFactory.senderEndPoint(channel, connectionId, libraryId, libraryBlockablePosition, this);
        senderEndPoints.add(senderEndPoint);

        final GatewaySession gatewaySession = new GatewaySession(
            connectionId,
            context,
            channel.remoteAddress(),
            connectionType,
            sessionKey,
            receiverEndPoint,
            senderEndPoint);

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

        final LiveLibraryInfo existingLibrary = idToLibrary.get(libraryId);
        if (existingLibrary != null)
        {
            existingLibrary.onHeartbeat(clock.time());

            return Pressure.apply(
                inboundPublication.saveControlNotification(libraryId, existingLibrary.sessions()));
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
            clock.time());

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

            DebugLogger.log(CLUSTER_MANAGEMENT, "Library %s - %s connected %n", libraryId, libraryName);

            return COMPLETE;
        });

        for (final GatewaySession gatewaySession : gatewaySessions.sessions())
        {
            unitsOfWork.add(
                    // TODO(Nick): UNK_SESSION is the wrong constant to use?
                () -> saveManageSession(ENGINE_LIBRARY_ID, gatewaySession, UNK_SESSION, UNK_SESSION, LIBRARY_NOTIFICATION));
        }

        return retryManager.firstAttempt(correlationId, new UnitOfWork(unitsOfWork));
    }

    public Action onApplicationHeartbeat(final int libraryId, final int aeronSessionId)
    {
        final LiveLibraryInfo library = idToLibrary.get(libraryId);
        if (library != null)
        {
            final long timeInMs = clock.time();
            DebugLogger.log(
                APPLICATION_HEARTBEAT, "Received Heartbeat from library %d at timeInMs %d%n", libraryId, timeInMs);
            library.onHeartbeat(timeInMs);

            return CONTINUE;
        }
        else
        {
            // We are not going to send the libraryName on every heartbeat so this library will not have a debug name.
            final Action action = onLibraryConnect(libraryId, "", libraryId, aeronSessionId);
            if (action == ABORT)
            {
                return ABORT;
            }

            return Pressure.apply(inboundPublication.saveControlNotification(libraryId, NO_SESSIONS));
        }
    }

    public Action onReleaseSession(
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final long correlationId,
        final SessionState state,
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
            return Pressure.apply(
                inboundPublication.saveReleaseSessionReply(
                    libraryId, SessionReplyStatus.UNKNOWN_LIBRARY, correlationId));
        }

        DebugLogger.log(
            CLUSTER_MANAGEMENT,
            "Releasing session %s with connectionId %s from library %s%n",
            sessionId,
            connectionId,
            libraryId);

        final GatewaySession session = libraryInfo.removeSession(connectionId);

        if (session == null)
        {
            return Pressure.apply(
                inboundPublication.saveReleaseSessionReply(
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
                LogonStatus.LIBRARY_NOTIFICATION));
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

        final LiveLibraryInfo libraryInfo = idToLibrary.get(libraryId);
        if (libraryInfo == null)
        {
            return Pressure.apply(
                inboundPublication.saveRequestSessionReply(
                    libraryId, SessionReplyStatus.UNKNOWN_LIBRARY, correlationId));
        }

        final GatewaySession gatewaySession = gatewaySessions.releaseBySessionId(sessionId);
        if (gatewaySession == null)
        {
            return Pressure.apply(
                inboundPublication.saveRequestSessionReply(
                    libraryId, SessionReplyStatus.UNKNOWN_SESSION, correlationId));
        }

        final Session session = gatewaySession.session();
        if (!session.isActive())
        {
            return Pressure.apply(
                inboundPublication.saveRequestSessionReply(
                    libraryId, SESSION_NOT_LOGGED_IN, correlationId));
        }

        final long connectionId = gatewaySession.connectionId();
        final int lastSentSeqNum = session.lastSentMsgSeqNum();
        final int lastRecvSeqNum = session.lastReceivedMsgSeqNum();
        final SessionState sessionState = session.state();
        final long logonTime = session.logonTime();
        gatewaySession.handoverManagementTo(libraryId, libraryInfo.librarySlowPeeker());
        libraryInfo.addSession(gatewaySession);

        DebugLogger.log(CLUSTER_MANAGEMENT, "Handing control for session %s to library %s%n", sessionId, libraryId);

        final List<Continuation> continuations = new ArrayList<>();

        continuations.add(() -> inboundPublication.saveManageSession(
                libraryId,
                connectionId,
                sessionId,
                lastSentSeqNum,
                lastRecvSeqNum,
                logonTime,
                LogonStatus.NEW,
                gatewaySession.slowStatus(),
                gatewaySession.connectionType(),
                sessionState,
                gatewaySession.heartbeatIntervalInS(),
                correlationId,
                gatewaySession.sequenceIndex(),
                session.compositeKey().localCompId(),
                session.compositeKey().localSubId(),
                session.compositeKey().localLocationId(),
                session.compositeKey().remoteCompId(),
                session.compositeKey().remoteSubId(),
                session.compositeKey().remoteLocationId(),
                gatewaySession.address()));

        catchupSession(
            continuations,
            libraryId,
            connectionId,
            correlationId,
            replayFromSequenceNumber,
            replayFromSequenceIndex,
            gatewaySession,
            lastRecvSeqNum);

        return retryManager.firstAttempt(correlationId, new UnitOfWork(continuations));
    }

    private long saveManageSession(
        final int libraryId,
        final GatewaySession gatewaySession,
        final int lastSentSeqNum,
        final int lastReceivedSeqNum,
        final LogonStatus logonstatus)
    {
        final CompositeKey compositeKey = gatewaySession.sessionKey();
        if (compositeKey != null)
        {
            final long connectionId = gatewaySession.connectionId();

            return inboundPublication.saveManageSession(
                    libraryId,
                                                        connectionId,
                                                        gatewaySession.sessionId(),
                                                        lastSentSeqNum,
                                                        lastReceivedSeqNum,
                                                        gatewaySession.session().logonTime(),
                                                        logonstatus,
                                                        gatewaySession.slowStatus(),
                                                        gatewaySession.connectionType(),
                                                        gatewaySession.session().state(),
                                                        gatewaySession.heartbeatIntervalInS(),
                                                        NO_CORRELATION_ID,
                                                        gatewaySession.sequenceIndex(),
                                                        compositeKey.localCompId(),
                                                        compositeKey.localSubId(),
                                                        compositeKey.localLocationId(),
                                                        compositeKey.remoteCompId(),
                                                        compositeKey.remoteSubId(),
                                                                     compositeKey.remoteLocationId(),
                                                                     gatewaySession.address());
        }

        return COMPLETE;
    }

    private void catchupSession(
        final List<Continuation> continuations,
        final int libraryId,
        final long connectionId,
        final long correlationId,
        final int replayFromSequenceNumber,
        final int replayFromSequenceIndex,
        final GatewaySession session,
        final int lastReceivedSeqNum)
    {
        if (replayFromSequenceNumber != NO_MESSAGE_REPLAY)
        {
            final int sequenceIndex = session.sequenceIndex();
            if (replayFromSequenceIndex > sequenceIndex ||
                (replayFromSequenceIndex == sequenceIndex && replayFromSequenceNumber > lastReceivedSeqNum))
            {
                continuations.add(() ->
                    sequenceNumberTooHigh(libraryId, correlationId, session));
                return;
            }

            continuations.add(
                new CatchupReplayer(
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
                    clock));
        }
        else
        {
            continuations.add(() ->
                CatchupReplayer.sendOk(inboundPublication, correlationId, session, libraryId));
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

    void onQueryLibraries(final QueryLibrariesCommand command)
    {
        final List<LibraryInfo> libraries = new ArrayList<>(idToLibrary.values());
        libraries.add(new EngineLibraryInfo(gatewaySessions));
        command.success(libraries);
    }

    void onResetSessionIds(final File backupLocation, final ResetSessionIdsCommand command)
    {
        schedule(
            new UnitOfWork(
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
                }
            )
        );
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
            command.error(new IllegalArgumentException(
                "Unknown Session: " + compositeKey));
        }
        else
        {
            command.complete(sessionId);
        }
    }

    private boolean sequenceNumbersNotReset()
    {
        return sentSequenceNumberIndex.lastKnownSequenceNumber(1) != UNK_SESSION
            || receivedSequenceNumberIndex.lastKnownSequenceNumber(1) != UNK_SESSION;
    }

    public void onClose()
    {
        Exceptions.closeAll(
            this::quiesce,
            inboundMessages,
            receiverEndPoints,
            senderEndPoints,
            channelSupplier);
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

        if (isClustered())
        {
            final Long2LongHashMap outboundClusterPositions = new Long2LongHashMap(CompletionPosition.MISSING_VALUE);
            final long position = clusterSubscription.position();
            outboundClusterPositions.put(OUTBOUND_LIBRARY_STREAM, position);
            outboundClusterCompletionPosition.complete(outboundClusterPositions);
        }
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
        final long position = inboundPublication.saveSlowStatusNotification(
            libraryId, connectionId, status);
        if (Pressure.isBackPressured(position))
        {
            toResend.put(connectionId, libraryId);
        }
    }
}
