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

import uk.co.real_logic.artio.messages.ConnectDecoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import static uk.co.real_logic.artio.GatewayProcess.NO_CONNECTION_ID;

// TODO: way of supplying connection ids
public class ReproductionTcpChannelSupplier extends TcpChannelSupplier
{
    private String address;

    class ReproductionTcpChannel extends TcpChannel
    {
        public ReproductionTcpChannel() throws IOException
        {
            super(address);
        }

        public SelectionKey register(final Selector sel, final int ops, final Object att) throws ClosedChannelException
        {
            // TODO: how much trouble will this being null cause me?
            return null;
        }

        public int write(final ByteBuffer src) throws IOException
        {
            // TODO: get the length of the buffer
            return src.remaining();
        }

        public int read(final ByteBuffer dst) throws IOException
        {
            return 0;
        }

        public void close()
        {
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
            handler.onNewChannel(timeInMs, new ReproductionTcpChannel());
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
        address = connectDecoder.address();
    }
}
