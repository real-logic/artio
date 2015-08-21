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
import uk.co.real_logic.agrona.concurrent.QueuedPipe;
import uk.co.real_logic.agrona.concurrent.SystemNanoClock;
import uk.co.real_logic.fix_gateway.Timer;
import uk.co.real_logic.fix_gateway.engine.ConnectionHandler;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.LibraryInfo;
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

import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;
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
    public static final int OUTBOUND_FRAGMENT_LIMIT = 5;
    private final Int2ObjectHashMap<LibraryInfo> idToLibrary = new Int2ObjectHashMap<>();
    private final Long2ObjectHashMap<SenderEndPoint> connectionToSenderEndpoint = new Long2ObjectHashMap<>();
    private final Consumer<AdminCommand> onAdminCommand = command -> command.execute(this);
    private final SystemNanoClock clock = new SystemNanoClock();
    private final Timer outboundTimer = new Timer("Outbound Framer", clock);
    private final DataSubscriber dataSubscriber;

    private final Selector selector;
    private final ServerSocketChannel listeningChannel;
    private final EngineConfiguration configuration;
    private final ConnectionHandler connectionHandler;
    private final Subscription outboundDataSubscription;
    private final Subscription replaySubscription;
    private final GatewayPublication inboundPublication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionIds sessionIds;
    private final QueuedPipe<AdminCommand> adminCommands;
    private final ReceiverEndPointPoller endPointPoller;

    private long nextConnectionId = (long) (Math.random() * Long.MAX_VALUE);

    public Framer(
        final EngineConfiguration configuration,
        final ConnectionHandler connectionHandler,
        final Subscription outboundLibrarySubscription,
        final Subscription replaySubscription,
        final GatewayPublication inboundPublication,
        final SessionIdStrategy sessionIdStrategy,
        final SessionIds sessionIds,
        final QueuedPipe<AdminCommand> adminCommands)
    {
        this.configuration = configuration;
        this.connectionHandler = connectionHandler;
        this.outboundDataSubscription = outboundLibrarySubscription;
        this.replaySubscription = replaySubscription;
        this.inboundPublication = inboundPublication;
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionIds = sessionIds;
        this.adminCommands = adminCommands;
        dataSubscriber = new DataSubscriber(this);

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
        return outboundDataSubscription.poll(dataSubscriber, OUTBOUND_FRAGMENT_LIMIT) +
               replaySubscription.poll(dataSubscriber, OUTBOUND_FRAGMENT_LIMIT) +
               pollSockets() +
               adminCommands.drain(onAdminCommand);
    }

    public void removeEndPoint(final ReceiverEndPoint receiverEndPoint)
    {
        connectionToSenderEndpoint.remove(receiverEndPoint.connectionId());
        endPointPoller.deregister(receiverEndPoint);
    }

    private int pollSockets() throws IOException
    {
        int totalBytesReceived = 0;
        int bytesReceived;
        do
        {
            bytesReceived = endPointPoller.pollEndPoints();
            totalBytesReceived += bytesReceived;
        }
        while (bytesReceived > 0);

        return totalBytesReceived + pollNewConnections();
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
                final long connectionId = this.nextConnectionId++;
                setupConnection(channel, connectionId, UNKNOWN);

                final String address = channel.getRemoteAddress().toString();
                inboundPublication.saveConnect(connectionId, address, ACCEPTOR);

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
                inboundPublication.saveError(DUPLICATE_SESSION, libraryId, "");
                return;
            }

            setupConnection(channel, connectionId, sessionId);
            inboundPublication.saveConnect(connectionId, address.toString(), INITIATOR);
            inboundPublication.saveLogon(connectionId, sessionId);
        }
        catch (final Exception e)
        {
            saveError(EXCEPTION, libraryId, e);
        }
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

    private void setupConnection(final SocketChannel channel, final long connectionId, final long sessionId)
        throws IOException
    {
        channel.setOption(TCP_NODELAY, true);
        if (configuration.receiverSocketBufferSize() > 0)
        {
            channel.setOption(SO_RCVBUF, configuration.receiverSocketBufferSize());
        }
        if (configuration.senderSocketBufferSize() > 0)
        {
            channel.setOption(SO_RCVBUF, configuration.senderSocketBufferSize());
        }
        channel.configureBlocking(false);

        final ReceiverEndPoint receiverEndPoint =
            connectionHandler.receiverEndPoint(channel, connectionId, sessionId, this);
        endPointPoller.register(receiverEndPoint);

        connectionToSenderEndpoint.put(connectionId, connectionHandler.senderEndPoint(channel, connectionId));
    }

    public void onRequestDisconnect(final long connectionId)
    {
        onDisconnect(connectionId);
    }

    public void onDisconnect(final long connectionId)
    {
        endPointPoller.deregister(connectionId);
    }

    public void onNewLibrary(final int libraryId)
    {
        final boolean isAcceptor = idToLibrary.isEmpty();
        idToLibrary.put(libraryId, new LibraryInfo(isAcceptor, libraryId));
    }

    public void onInactiveLibrary(final int libraryId)
    {
        idToLibrary.remove(libraryId);
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
