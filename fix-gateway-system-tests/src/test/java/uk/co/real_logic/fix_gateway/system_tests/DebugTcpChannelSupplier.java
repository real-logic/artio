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
package uk.co.real_logic.fix_gateway.system_tests;

import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.framer.TcpChannel;
import uk.co.real_logic.fix_gateway.engine.framer.TcpChannelSupplier;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

/**
 * Hook for testing interaction with different networking conditions.
 *
 * NB: this class is not thread-safe and take care to ensure that your tests don't use it
 * concurrently with new connections being established.
 */
public class DebugTcpChannelSupplier extends TcpChannelSupplier
{
    private final ArrayList<TcpChannel> channels = new ArrayList<>();
    private boolean isEnabled = true;

    public DebugTcpChannelSupplier(final EngineConfiguration configuration)
    {
        super(configuration);
    }

    protected TcpChannel newTcpChannel(final SocketChannel channel) throws IOException
    {
        final TcpChannel tcpChannel = new TcpChannel(channel);
        channels.add(tcpChannel);
        return tcpChannel;
    }

    public void disable()
    {
        isEnabled = false;
        channels.forEach(TcpChannel::close);
        channels.clear();
    }

    public void enable()
    {
        isEnabled = true;

        if (!channels.isEmpty())
        {
            throw new IllegalStateException(
                "Tried enabling channel supplier, but channels were already connected");
        }
    }

    public int pollSelector(final long timeInMs, final NewChannelHandler handler) throws IOException
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

    public void open(final InetSocketAddress address, final InitiatedChannelHandler handler) throws IOException
    {
        if (isEnabled)
        {
            super.open(address, handler);
        }
        else
        {
            handler.onInitiatedChannel(null, new IOException("Unable to connect"));
        }
    }
}
