/*
 * Copyright 2015 Real Logic Ltd.
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
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.EpochClock;
import uk.co.real_logic.agrona.concurrent.QueuedPipe;
import uk.co.real_logic.agrona.concurrent.SystemNanoClock;
import uk.co.real_logic.fix_gateway.LivenessDetector;
import uk.co.real_logic.fix_gateway.Timer;
import uk.co.real_logic.fix_gateway.engine.ConnectionHandler;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.library.session.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.GatewayError;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.streams.DataSubscriber;
import uk.co.real_logic.fix_gateway.streams.GatewayPublication;

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

/**
 * Handles incoming connections from clients and outgoing connections to exchanges.
 */
public class Framer implements Agent, SessionHandler
{
    public static final int NO_ACCEPTOR = -1;

    private final Int2ObjectHashMap<LibraryInfo> idToLibrary = new Int2ObjectHashMap<>();
    private final Long2ObjectHashMap<SenderEndPoint> connectionToSenderEndpoint = new Long2ObjectHashMap<>();
    private final Consumer<AdminCommand> onAdminCommand = command -> command.execute(this);
    private final Consumer<ReceiverEndPoint> removeSenderEndPointFunc = this::removeSenderEndPoint;
    private final Depressurizer sendOutboundMessagesFunc = this::sendOutboundMessages;
    private final EpochClock clock;
    private final Timer outboundTimer = new Timer("Outbound Framer", new SystemNanoClock());
    private final DataSubscriber dataSubscriber = new DataSubscriber(this);

    private final Selector selector;
    private final ServerSocketChannel listeningChannel;
    private final ReceiverEndPointPoller endPointPoller;

    private final EngineConfiguration configuration;
    private final ConnectionHandler connectionHandler;
    private final Subscription outboundDataSubscription;
    private final Subscription replaySubscription;
    private final GatewayPublication inboundPublication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionIds sessionIds;
    private final QueuedPipe<AdminCommand> adminCommands;
    private final int inboundBytesReceivedLimit;
    private final int outboundLibraryFragmentLimit;
    private final int replayFragmentLimit;

    private long nextConnectionId = (long) (Math.random() * Long.MAX_VALUE);
    private int acceptorLibraryId = NO_ACCEPTOR;

    public Framer(
        final EpochClock clock,
        final EngineConfiguration configuration,
        final ConnectionHandler connectionHandler,
        final Subscription outboundLibrarySubscription,
        final Subscription replaySubscription,
        final GatewayPublication inboundPublication,
        final SessionIdStrategy sessionIdStrategy,
        final SessionIds sessionIds,
        final QueuedPipe<AdminCommand> adminCommands)
    {
        this.clock = clock;
        this.configuration = configuration;
        this.connectionHandler = connectionHandler;
        this.outboundDataSubscription = outboundLibrarySubscription;
        this.replaySubscription = replaySubscription;
        this.inboundPublication = inboundPublication;
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionIds = sessionIds;
        this.adminCommands = adminCommands;

        this.outboundLibraryFragmentLimit = configuration.outboundLibraryFragmentLimit();
        this.replayFragmentLimit = configuration.replayFragmentLimit();
        this.inboundBytesReceivedLimit = configuration.inboundBytesReceivedLimit();

        try
        {
            listeningChannel = ServerSocketChannel.open();
            listeningChannel.bind(configuration.bindAddress()).configureBlocking(false);

            selector = Selector.open();
            listeningChannel.register(selector, SelectionKey.OP_ACCEPT);

            endPointPoller = new ReceiverEndPointPoller();
        }
        catch (final IOException ex)
        {
            throw new IllegalArgumentException(ex);
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
        return replaySubscription.poll(dataSubscriber, replayFragmentLimit);
    }

    private int sendOutboundMessages()
    {
        return outboundDataSubscription.poll(dataSubscriber, outboundLibraryFragmentLimit);
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
                endPointPoller.deregisterLibrary(library.libraryId(), removeSenderEndPointFunc);
                if (library.isAcceptor())
                {
                    acceptorLibraryId = NO_ACCEPTOR;
                }
            }
        }

        return total;
    }

    public void removeEndPoint(final ReceiverEndPoint receiverEndPoint)
    {
        endPointPoller.deregisterEndPoint(receiverEndPoint);
        removeSenderEndPoint(receiverEndPoint);
    }

    private void removeSenderEndPoint(final ReceiverEndPoint receiverEndPoint)
    {
        final long id = receiverEndPoint.connectionId();
        final SenderEndPoint senderEndPoint = connectionToSenderEndpoint.remove(id);
        senderEndPoint.close();
    }

    private int pollEndPoints() throws IOException
    {
        final int inboundBytesReceivedLimit = this.inboundBytesReceivedLimit;

        int totalBytesReceived = 0;
        int bytesReceived;
        do
        {
            bytesReceived = endPointPoller.pollEndPoints();
            totalBytesReceived += bytesReceived;
        }
        while (bytesReceived > 0 && totalBytesReceived < inboundBytesReceivedLimit);

        return totalBytesReceived;
    }

    private int pollNewConnections() throws IOException
    {
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
                    inboundPublication.saveConnect(connectionId, address, acceptorLibraryId, ACCEPTOR);
                }

                it.remove();
            }
        }

        return newConnections;
    }

    public void onInitiateConnection(final int libraryId,
                                     final int port,
                                     final String host,
                                     final String senderCompId,
                                     final String senderSubId,
                                     final String senderLocationId,
                                     final String targetCompId)
    {
        final LibraryInfo library = idToLibrary.get(libraryId);
        if (library == null)
        {
            saveError(UNKNOWN_LIBRARY, libraryId);
            return;
        }

        try
        {
            SocketChannel channel;
            InetSocketAddress address;
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
            inboundPublication.saveConnect(connectionId, address.toString(), libraryId, INITIATOR);
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
        if (TIME_MESSAGES)
        {
            outboundTimer.recordSince(timestamp);
        }

        final SenderEndPoint endPoint = connectionToSenderEndpoint.get(connectionId);
        if (endPoint != null)
        {
            endPoint.onFramedMessage(buffer, offset, length);
        }
    }

    private void setupConnection(final SocketChannel channel,
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
            connectionHandler.receiverEndPoint(channel, connectionId, sessionId, this, libraryId,
            sendOutboundMessagesFunc);
        endPointPoller.register(receiverEndPoint);

        final SenderEndPoint senderEndPoint = connectionHandler.senderEndPoint(channel, connectionId);
        connectionToSenderEndpoint.put(connectionId, senderEndPoint);

        idToLibrary.get(libraryId).onSessionConnected(new SessionInfo(
            connectionId,
            channel.getRemoteAddress().toString()
        ));
    }

    public void onRequestDisconnect(final int libraryId, final long connectionId)
    {
        onDisconnect(libraryId, connectionId);
    }

    public void onDisconnect(final int libraryId, final long connectionId)
    {
        endPointPoller.deregisterConnection(connectionId, removeSenderEndPointFunc);
    }

    public void onApplicationHeartbeat(final int libraryId)
    {
        LibraryInfo library = idToLibrary.get(libraryId);
        if (library == null)
        {
            final LivenessDetector livenessDetector = new LivenessDetector(
                inboundPublication,
                libraryId,
                configuration.replyTimeoutInMs(),
                clock.time(),
                true);

            library = new LibraryInfo(acceptorLibraryId == NO_ACCEPTOR, libraryId, livenessDetector);
            if (library.isAcceptor())
            {
                acceptorLibraryId = libraryId;
            }
            idToLibrary.put(libraryId, library);
        }
        else
        {
            library.onHeartbeat(clock.time());
        }
    }

    public void onQueryLibraries(final QueryLibraries queryLibraries)
    {
        final List<LibraryInfo> libraries = new ArrayList<>(idToLibrary.values());
        queryLibraries.respond(libraries);
    }

    public void onClose()
    {
        endPointPoller.close();
        close(selector);
        close(listeningChannel);
    }

    public String roleName()
    {
        return "Framer";
    }
}
