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

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.*;
import uk.co.real_logic.fix_gateway.LivenessDetector;
import uk.co.real_logic.fix_gateway.ReliefValve;
import uk.co.real_logic.fix_gateway.Timer;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.fix_gateway.library.session.ProcessProtocolHandler;
import uk.co.real_logic.fix_gateway.library.session.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.ConnectionType;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.messages.GatewayError;
import uk.co.real_logic.fix_gateway.messages.SessionReplyStatus;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.streams.GatewayPublication;
import uk.co.real_logic.fix_gateway.streams.ProcessProtocolSubscription;
import uk.co.real_logic.fix_gateway.streams.SessionSubscription;

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
import static uk.co.real_logic.agrona.CloseHelper.close;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.TIME_MESSAGES;
import static uk.co.real_logic.fix_gateway.library.session.Session.UNKNOWN;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.ACCEPTOR;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.fix_gateway.messages.GatewayError.*;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.OK;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.UNKNOWN_SESSION;
import static uk.co.real_logic.fix_gateway.streams.Streams.UNKNOWN_TEMPLATE;

/**
 * Handles incoming connections from clients and outgoing connections to exchanges.
 */
public class Framer implements Agent, ProcessProtocolHandler, SessionHandler
{
    public static final int NO_ACCEPTOR = -1;

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
    private final ProcessProtocolSubscription processProtocolSubscription = new ProcessProtocolSubscription(this);
    private final FragmentHandler outboundSubscription = (buffer, offset, length, header) ->
    {
        if (sessionSubscription.readFragment(buffer, offset, header) == UNKNOWN_TEMPLATE)
        {
            processProtocolSubscription.onFragment(buffer, offset, length, header);
        }
    };

    private final boolean hasBindAddress;
    private final Selector selector;
    private final ServerSocketChannel listeningChannel;
    private final ReceiverEndPoints receiverEndPoints = new ReceiverEndPoints();
    private final SenderEndPoints senderEndPoints = new SenderEndPoints();

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

    private long nextConnectionId = (long)(Math.random() * Long.MAX_VALUE);
    private int acceptorLibraryId = NO_ACCEPTOR;

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
        final GatewaySessions gatewaySessions)
    {
        this.clock = clock;
        this.configuration = configuration;
        this.connectionHandler = connectionHandler;
        this.outboundDataSubscription = outboundLibrarySubscription;
        this.replaySubscription = replaySubscription;
        this.gatewaySessions = gatewaySessions;
        this.inboundPublication = connectionHandler.inboundPublication(sendOutboundMessagesFunc);
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionIds = sessionIds;
        this.adminCommands = adminCommands;
        this.sentSequenceNumberIndex = sentSequenceNumberIndex;
        this.receivedSequenceNumberIndex = receivedSequenceNumberIndex;
        this.idleStrategy = configuration.framerIdleStrategy();

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
        return sendOutboundMessages() +
               sendReplayMessages() +
               pollEndPoints() +
               pollNewConnections() +
               pollLibraries() +
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

    private int pollLibraries()
    {
        final long timeInMs = clock.time();
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
                if (library.isAcceptor())
                {
                    acceptorLibraryId = NO_ACCEPTOR;
                }
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
                if (acceptorLibraryId == NO_ACCEPTOR)
                {
                    saveError(UNKNOWN_LIBRARY, NO_ACCEPTOR);
                    channel.close();
                }
                else
                {
                    final long connectionId = this.nextConnectionId++;
                    setupConnection(channel, connectionId, UNKNOWN, acceptorLibraryId);

                    final String address = channel.getRemoteAddress().toString();
                    inboundPublication.saveConnect(connectionId, address, acceptorLibraryId, ACCEPTOR,
                        SequenceNumberIndexReader.UNKNOWN_SESSION, SequenceNumberIndexReader.UNKNOWN_SESSION);
                }

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
        final Header header)
    {
        final LibraryInfo library = idToLibrary.get(libraryId);
        if (library == null)
        {
            saveError(UNKNOWN_LIBRARY, libraryId);
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

            final Object sessionKey = sessionIdStrategy.onInitiatorLogon(
                senderCompId, senderSubId, senderLocationId, targetCompId);
            final long sessionId = sessionIds.onLogon(sessionKey);
            if (sessionId == SessionIds.DUPLICATE_SESSION)
            {
                saveError(DUPLICATE_SESSION, libraryId);
                return;
            }

            setupConnection(channel, connectionId, sessionId, libraryId);

            while (!sentSequenceNumberIndex.hasIndexedUpTo(header))
            {
                idleStrategy.idle();
            }
            idleStrategy.reset();

            final int lastSentSequenceNumber = sentSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
            final int lastReceivedSequenceNumber = receivedSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
            inboundPublication.saveConnect(connectionId, address.toString(), libraryId, INITIATOR,
                lastSentSequenceNumber, lastReceivedSequenceNumber);
            inboundPublication.saveLogon(libraryId, connectionId, sessionId);
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

    private void setupConnection(
        final SocketChannel channel,
        final long connectionId,
        final long sessionId,
        final int libraryId)
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
                sendOutboundMessagesFunc, sentSequenceNumberIndex, receivedSequenceNumberIndex);
        receiverEndPoints.add(receiverEndPoint);

        final SenderEndPoint senderEndPoint =
            connectionHandler.senderEndPoint(channel, connectionId, libraryId, this, pollEndpointsFunc);
        senderEndPoints.add(senderEndPoint);

        idToLibrary.get(libraryId).onSessionConnected(new GatewaySession(
            connectionId,
            sessionId,
            channel.getRemoteAddress().toString(),
            receiverEndPoint
        ));
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
    }

    public void onLibraryConnect(final int libraryId, final ConnectionType connectionType)
    {
        final long timeInMs = clock.time();
        if (idToLibrary.containsKey(libraryId))
        {
            saveError(DUPLICATE_LIBRARY_ID, libraryId);
            return;
        }

        final boolean isAcceptor = connectionType == ACCEPTOR;
        if (isAcceptor && acceptorLibraryId != NO_ACCEPTOR)
        {
            saveError(DUPLICATE_ACCEPTOR, libraryId);
            return;
        }

        final LivenessDetector livenessDetector = LivenessDetector.forEngine(
            inboundPublication,
            libraryId,
            configuration.replyTimeoutInMs(),
            timeInMs);

        final LibraryInfo library = new LibraryInfo(isAcceptor, libraryId, livenessDetector);
        if (isAcceptor)
        {
            acceptorLibraryId = libraryId;
        }
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

    public void onReleaseSession(final int libraryId, final long connectionId, final long correlationId)
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
            inboundPublication.saveReleaseSessionReply(UNKNOWN_SESSION, correlationId);
            return;
        }

        gatewaySessions.startManaging(session);

        inboundPublication.saveReleaseSessionReply(OK, correlationId);
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
