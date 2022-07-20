/*
 * Copyright 2022 Monotonic Ltd.
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

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.artio.engine.ReproductionMessageHandler;
import uk.co.real_logic.artio.messages.ConnectDecoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

public class ReproductionTcpChannelSupplier extends TcpChannelSupplier
{
    private final Long2ObjectHashMap<ReproductionTcpChannel> connectionIdToChannel = new Long2ObjectHashMap<>();

    private final ReproductionMessageHandler reproductionMessageHandler;

    private long connectionId;
    private String address;
    private Runnable endOperation;

    public ReproductionTcpChannelSupplier(
        final ReproductionMessageHandler reproductionMessageHandler)
    {
        this.reproductionMessageHandler = reproductionMessageHandler;
    }

    public void registerEndOperation(final Runnable endOperation)
    {
        this.endOperation = endOperation;
    }

    class ReproductionTcpChannel extends TcpChannel
    {
        private final ExpandableArrayBuffer reproductionBuffer = new ExpandableArrayBuffer();

        private final long connectionId;

        private int length;

        ReproductionTcpChannel(final long connectionId) throws IOException
        {
            super(address);
            this.connectionId = connectionId;
        }

        public SelectionKey register(final Selector sel, final int ops, final Object att)
            throws ClosedChannelException
        {
            return null; // we null-check elsewhere so this is safe
        }

        public int write(final ByteBuffer src) throws IOException
        {
            final int remaining = src.remaining();
            reproductionMessageHandler.onMessage(connectionId, src);
            return remaining;
        }

        public int read(final ByteBuffer dst) throws IOException
        {
            final int length = this.length;
            if (length > 0)
            {
                System.out.println("READ: '" + reproductionBuffer.getStringWithoutLengthAscii(0, length) + "'");
                reproductionBuffer.getBytes(0, dst, length);
                this.length = 0;
                endOperation.run();
                return length;
            }

            return 0;
        }

        public void close()
        {
        }

        public boolean enqueueMessage(
            final DirectBuffer buffer, final int initialOffset, final int fullLength, final int messageOffset,
            final int length)
        {
            if (this.length != 0)
            {
                return false;
            }

            reproductionBuffer.putBytes(0, buffer, initialOffset + messageOffset, length);
            this.length = length;
            return true;
        }
    }

    public void open(final InetSocketAddress address, final InitiatedChannelHandler channelHandler) throws IOException
    {
    }

    public void stopConnecting(final InetSocketAddress address) throws IOException
    {
    }

    public int pollSelector(final long timeInMs, final NewChannelHandler handler) throws IOException
    {
        if (address != null)
        {
            final ReproductionTcpChannel channel = new ReproductionTcpChannel(connectionId);
            connectionIdToChannel.put(connectionId, channel);
            handler.onNewChannel(timeInMs, channel);
            address = null;
        }

        return 0;
    }

    public void unbind() throws IOException
    {
    }

    public void bind() throws IOException
    {
    }

    public void close() throws Exception
    {
    }

    public void enqueueConnect(final ConnectDecoder connectDecoder)
    {
        connectionId = connectDecoder.connection();
        address = connectDecoder.address();
    }

    public boolean enqueueMessage(
        final long connectionId,
        final DirectBuffer buffer,
        final int initialOffset,
        final int fullLength,
        final int messageOffset,
        final int length)
    {
        final ReproductionTcpChannel channel = connectionIdToChannel.get(connectionId);
        if (channel != null)
        {
            return channel.enqueueMessage(buffer, initialOffset, fullLength, messageOffset, length);
        }

        return false;
    }
}
