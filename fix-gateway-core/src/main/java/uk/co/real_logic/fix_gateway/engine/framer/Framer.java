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
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.*;
import uk.co.real_logic.fix_gateway.LivenessDetector;
import uk.co.real_logic.fix_gateway.ReliefValve;
import uk.co.real_logic.fix_gateway.Timer;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.logger.ReplayQuery;
import uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.fix_gateway.library.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.protocol.*;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

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

import static java.net.StandardSocketOptions.*;
import static org.agrona.CloseHelper.close;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.TIME_MESSAGES;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.GATEWAY_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.ACCEPTOR;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.fix_gateway.messages.GatewayError.*;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.*;
import static uk.co.real_logic.fix_gateway.messages.SessionState.CONNECTED;
import static uk.co.real_logic.fix_gateway.session.Session.UNKNOWN;

/**
 * Handles incoming connections from clients and outgoing connections to exchanges.
 */
public class Framer implements Agent, EngineProtocolHandler, SessionHandler
{

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

    private final EpochClock clock;
    private final Timer outboundTimer = new Timer("Outbound Framer", new SystemNanoClock());
    private final Timer sendTimer = new Timer("Send", new SystemNanoClock());
    private final SessionSubscription sessionSubscription = new SessionSubscription(this);
    private final FragmentHandler outboundSubscription =
        sessionSubscription.andThen(new EngineProtocolSubscription(this));

    private final boolean hasBindAddress;
    private final Selector selector;
    private final ServerSocketChannel listeningChannel;
    private final ReceiverEndPoints receiverEndPoints = new ReceiverEndPoints();
    private final SenderEndPoints senderEndPoints;

    private final EngineConfiguration configuration;
    private final ConnectionHandler connectionHandler;
    private final Subscription outboundDataSubscription;
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

    private long nextConnectionId = (long)(Math.random() * Long.MAX_VALUE);

    public Framer(
        final EpochClock clock,
        final EngineConfiguration configuration,
        final ConnectionHandler connectionHandler,
        final Subscription outboundLibrarySubscription,
        final Subscription replaySubscription,
        final QueuedPipe<AdminCommand> adminCommands,
        final SessionIdStrategy sessionIdStrategy,
        final SessionIds sessionIds,
        final SequenceNumberIndexReader sentSequenceNumberIndex,
        final SequenceNumberIndexReader receivedSequenceNumberIndex,
        final GatewaySessions gatewaySessions,
        final ReplayQuery inboundMessages,
        final ErrorHandler errorHandler)
    {
        this.clock = clock;
        this.configuration = configuration;
        this.connectionHandler = connectionHandler;
        this.outboundDataSubscription = outboundLibrarySubscription;
        this.replaySubscription = replaySubscription;
        this.gatewaySessions = gatewaySessions;
        this.inboundMessages = inboundMessages;
        this.errorHandler = errorHandler;
        this.inboundPublication = connectionHandler.inboundPublication(sendOutboundMessagesFunc);
        senderEndPoints = new SenderEndPoints(inboundPublication);
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionIds = sessionIds;
        this.adminCommands = adminCommands;
        this.sentSequenceNumberIndex = sentSequenceNumberIndex;
        this.receivedSequenceNumberIndex = receivedSequenceNumberIndex;
        this.idleStrategy = configuration.framerIdleStrategy();
        catchupReplayer = new CatchupReplayer(inboundPublication);

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
    }

    @Override
    public int doWork() throws Exception
    {
        final long timeInMs = clock.time();
        return sendOutboundMessages() +
               sendReplayMessages() +
               pollEndPoints() +
               pollNewConnections() +
               pollLibraries(timeInMs) +
               gatewaySessions.pollSessions(timeInMs) +
               adminCommands.drain(onAdminCommand);
    }

    private int sendReplayMessages()
    {
        return replaySubscription.poll(sessionSubscription, replayFragmentLimit);
    }

    private int sendOutboundMessages()
    {
        return outboundDataSubscription.poll(outboundSubscription, outboundLibraryFragmentLimit);
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
                final int libraryId = library.libraryId();
                receiverEndPoints.removeLibrary(libraryId);
                senderEndPoints.removeLibrary(libraryId);
            }
        }

        return total;
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

    private int pollNewConnections() throws IOException
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

                gatewaySessions.acquire(
                    session,
                    SessionState.CONNECTED,
                    configuration.defaultHeartbeatInterval(),
                    SequenceNumberIndexReader.UNKNOWN_SESSION,
                    SequenceNumberIndexReader.UNKNOWN_SESSION,
                    null,
                    null);

                final String address = channel.getRemoteAddress().toString();
                inboundPublication.saveConnect(connectionId, address);

                it.remove();
            }
        }

        return newConnections;
    }

    public void onInitiateConnection(
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
        final Header header)
    {
        final LibraryInfo library = idToLibrary.get(libraryId);
        if (library == null)
        {
            saveError(GatewayError.UNKNOWN_LIBRARY, libraryId);
            return;
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
                return;
            }

            final long connectionId = this.nextConnectionId++;

            final CompositeKey sessionKey = sessionIdStrategy.onLogon(
                senderCompId, senderSubId, senderLocationId, targetCompId);
            final long sessionId = sessionIds.onLogon(sessionKey);
            if (sessionId == SessionIds.DUPLICATE_SESSION)
            {
                saveError(DUPLICATE_SESSION, libraryId);
                return;
            }

            final boolean resetSeqNumbers = sequenceNumberType == SequenceNumberType.TRANSIENT;
            final GatewaySession session =
                setupConnection(channel, connectionId, sessionId, sessionKey, libraryId, INITIATOR, resetSeqNumbers);

            idToLibrary.get(libraryId).addSession(session);

            sentSequenceNumberIndex.awaitingIndexingUpTo(header, idleStrategy);

            final int lastSentSequenceNumber = sentSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
            final int lastReceivedSequenceNumber = receivedSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
            session.onLogon(sessionId, sessionKey, username, password);
            inboundPublication.saveManageConnection(connectionId, address.toString(), libraryId, INITIATOR,
                lastSentSequenceNumber, lastReceivedSequenceNumber, CONNECTED);
            inboundPublication.saveLogon(
                libraryId, connectionId, sessionId,
                lastSentSequenceNumber, lastReceivedSequenceNumber,
                senderCompId, senderSubId, senderLocationId, targetCompId,
                username, password);
        }
        catch (final Exception e)
        {
            saveError(EXCEPTION, libraryId, e);
        }
    }

    private void saveError(final GatewayError error, final int libraryId)
    {
        inboundPublication.saveError(error, libraryId, "");
    }

    private void saveError(final GatewayError error, final int libraryId, final Exception e)
    {
        final String message = e.getMessage();
        inboundPublication.saveError(error, libraryId, message == null ? "" : message);
    }

    public void onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int messageType,
        final long timestamp)
    {
        long now = 0;
        if (TIME_MESSAGES)
        {
            now = outboundTimer.recordSince(timestamp);
        }

        senderEndPoints.onMessage(connectionId, buffer, offset, length);

        if (TIME_MESSAGES)
        {
            sendTimer.recordSince(now);
        }
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
            connectionHandler.senderEndPoint(channel, connectionId, libraryId, this, pollEndpointsFunc);
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

    public void onRequestDisconnect(final int libraryId, final long connectionId)
    {
        onDisconnect(libraryId, connectionId, null);
    }

    public void onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
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
    }

    public void onLibraryConnect(final int libraryId)
    {
        final long timeInMs = clock.time();
        if (idToLibrary.containsKey(libraryId))
        {
            saveError(DUPLICATE_LIBRARY_ID, libraryId);
            return;
        }

        final LivenessDetector livenessDetector = LivenessDetector.forEngine(
            inboundPublication,
            libraryId,
            configuration.replyTimeoutInMs(),
            timeInMs);

        final LibraryInfo library = new LibraryInfo(libraryId, livenessDetector);
        idToLibrary.put(libraryId, library);
    }

    public void onApplicationHeartbeat(final int libraryId)
    {
        final LibraryInfo library = idToLibrary.get(libraryId);
        if (library != null)
        {
            final long timeInMs = clock.time();
            library.onHeartbeat(timeInMs);
        }
    }

    public void onReleaseSession(
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
            return;
        }

        final GatewaySession session = libraryInfo.removeSession(connectionId);
        if (session == null)
        {
            inboundPublication.saveReleaseSessionReply(SessionReplyStatus.UNKNOWN_SESSION, correlationId);
            return;
        }

        gatewaySessions.acquire(
            session,
            state,
            heartbeatIntervalInMs,
            lastSentSequenceNumber,
            lastReceivedSequenceNumber,
            username,
            password);

        inboundPublication.saveReleaseSessionReply(OK, correlationId);
    }

    public void onRequestSession(
        final int libraryId,
        final long connectionId,
        final long correlationId,
        int replayFromSequenceNumber)
    {
        final LibraryInfo libraryInfo = idToLibrary.get(libraryId);
        if (libraryInfo == null)
        {
            inboundPublication.saveRequestSessionReply(SessionReplyStatus.UNKNOWN_LIBRARY, correlationId);
            return;
        }

        final GatewaySession gatewaySession = gatewaySessions.release(connectionId);
        if (gatewaySession == null)
        {
            inboundPublication.saveRequestSessionReply(SessionReplyStatus.UNKNOWN_SESSION, correlationId);
            return;
        }

        final Session session = gatewaySession.session();
        final int lastSentSeqNum = session.lastSentMsgSeqNum();
        final int lastReceivedSeqNum = session.lastReceivedMsgSeqNum();
        final SessionState sessionState = session.state();
        final CompositeKey compositeKey = gatewaySession.compositeKey();
        gatewaySession.handoverManagementTo(libraryId);
        libraryInfo.addSession(gatewaySession);

        inboundPublication.saveManageConnection(
            connectionId,
            gatewaySession.address(),
            libraryId,
            gatewaySession.connectionType(),
            lastSentSeqNum,
            lastReceivedSeqNum,
            sessionState);

        if (compositeKey != null)
        {
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
                password);
        }

        if (replayFromSequenceNumber != NO_MESSAGE_REPLAY)
        {
            final long sessionId = session.id();
            if (sessionId == Session.UNKNOWN)
            {
                inboundPublication.saveRequestSessionReply(SESSION_NOT_LOGGED_IN, correlationId);
                return;
            }
            final int expectedNumberOfMessages = lastReceivedSeqNum - replayFromSequenceNumber;

            if (expectedNumberOfMessages < 0)
            {
                errorHandler.onError(new IllegalStateException(String.format(
                    "Sequence Number too high for %d, wanted %d, but we've only archived %d",
                    correlationId,
                    replayFromSequenceNumber,
                    lastReceivedSeqNum)));
                inboundPublication.saveRequestSessionReply(SEQUENCE_NUMBER_TOO_HIGH, correlationId);
                return;
            }

            inboundPublication.saveCatchup(libraryId, connectionId, expectedNumberOfMessages);

            catchupReplayer.libraryId(libraryId);
            final int numberOfMessages =
                inboundMessages.query(
                    catchupReplayer,
                    sessionId,
                    replayFromSequenceNumber + 1, // convert to inclusive numbering
                    lastReceivedSeqNum);

            if (numberOfMessages != expectedNumberOfMessages)
            {
                errorHandler.onError(new IllegalStateException(String.format(
                    "Failed to read correct number of messages for %d, expected %d, read %d",
                    correlationId,
                    expectedNumberOfMessages,
                    numberOfMessages)));
                inboundPublication.saveRequestSessionReply(MISSING_MESSAGES, correlationId);
                return;
            }
        }

        inboundPublication.saveRequestSessionReply(OK, correlationId);
    }

    void onQueryLibraries(final QueryLibrariesCommand queryLibrariesCommand)
    {
        final List<LibraryInfo> libraries = new ArrayList<>(idToLibrary.values());
        queryLibrariesCommand.success(libraries);
    }

    void onGatewaySessions(final GatewaySessionsCommand gatewaySessionsCommand)
    {
        gatewaySessionsCommand.success(new ArrayList<>(gatewaySessions.sessions()));
    }

    void resetSessionIds(final File backupLocation, final ResetSessionIdsCommand command)
    {
        try
        {
            sessionIds.reset(backupLocation);
            command.success();
        }
        catch (Exception e)
        {
            command.onError(e);
        }
    }

    public void onClose()
    {
        receiverEndPoints.close();
        senderEndPoints.close();
        close(selector);
        close(listeningChannel);
    }

    public String roleName()
    {
        return "Framer";
    }

}
