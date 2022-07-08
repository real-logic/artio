/*
 * Copyright 2015-2022 Real Logic Limited.
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
package uk.co.real_logic.artio.system_tests;

import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.framer.DefaultTcpChannel;
import uk.co.real_logic.artio.engine.framer.DefaultTcpChannelSupplier;
import uk.co.real_logic.artio.engine.framer.TcpChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

/**
 * Hook for testing interaction with different networking conditions.
 */
public class DebugTcpChannelSupplier extends DefaultTcpChannelSupplier
{
    private final ArrayList<TcpChannel> channels = new ArrayList<>();
    private boolean isEnabled = true;

    public DebugTcpChannelSupplier(final EngineConfiguration configuration)
    {
        super(configuration);
    }

    protected synchronized TcpChannel newTcpChannel(final SocketChannel channel) throws IOException
    {
        final TcpChannel tcpChannel = new DefaultTcpChannel(channel);
        channels.add(tcpChannel);
        return tcpChannel;
    }

    public synchronized void disable()
    {
        isEnabled = false;
        channels.forEach(TcpChannel::close);
        channels.clear();
    }

    public synchronized void enable()
    {
        isEnabled = true;

        if (!channels.isEmpty())
        {
            throw new IllegalStateException(
                "Tried enabling channel supplier, but channels were already connected");
        }
    }

    public synchronized int pollSelector(final long timeInMs, final NewChannelHandler handler) throws IOException
    {
        if (isEnabled)
        {
            return super.pollSelector(timeInMs, handler);
        }
        else
        {
            return super.pollSelector(timeInMs, (ignore, socketChannel) -> socketChannel.close());
        }
    }

    public synchronized void open(
        final InetSocketAddress address,
        final InitiatedChannelHandler handler)
        throws IOException
    {
        if (isEnabled)
        {
            super.open(address, handler);
        }
        else
        {
            // Deliberately blank - black hole the connection
        }
    }
}
