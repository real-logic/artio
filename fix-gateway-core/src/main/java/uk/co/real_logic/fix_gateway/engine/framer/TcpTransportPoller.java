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

import uk.co.real_logic.aeron.driver.media.UdpChannelTransport;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.collections.ArrayUtil;
import uk.co.real_logic.agrona.nio.TransportPoller;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public class TcpTransportPoller extends TransportPoller
{
    private TcpChannelTransport[] transports = new TcpChannelTransport[0];

    public void register(final TcpChannelTransport channelTransport)
    {
        try
        {
            transports = ArrayUtil.add(transports, channelTransport);
            channelTransport.register(selector);
        }
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    public void deregister(final TcpChannelTransport channelTransport)
    {
        transports = ArrayUtil.remove(transports, channelTransport);
    }

    public int pollTransports()
    {
        int bytesReceived = 0;
        try
        {
            final TcpChannelTransport[] transports = this.transports;
            final int numTransports = transports.length;
            if (numTransports <= ITERATION_THRESHOLD)
            {
                for (int i = numTransports - 1; i >= 0; i--)
                {
                    bytesReceived += transports[i].pollForData();
                }
            }
            else
            {
                selector.selectNow();

                final SelectionKey[] keys = selectedKeySet.keys();
                for (int i = selectedKeySet.size() - 1; i >= 0; i--)
                {
                    bytesReceived += ((UdpChannelTransport)keys[i].attachment()).pollForData();
                }

                selectedKeySet.reset();
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
        return bytesReceived;
    }
}
