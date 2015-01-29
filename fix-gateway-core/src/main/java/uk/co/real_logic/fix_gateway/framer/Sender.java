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
package uk.co.real_logic.fix_gateway.framer;

import uk.co.real_logic.aeron.common.Agent;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.fix_gateway.commands.SenderCommand;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Sends outbound data
 */
public class Sender implements Agent
{

    private final Consumer<SenderCommand> onCommandFunc = this::onCommand;
    private final List<SocketChannel> channels = new ArrayList<>();

    private final OneToOneConcurrentArrayQueue<SenderCommand> commandQueue;

    public Sender(final OneToOneConcurrentArrayQueue<SenderCommand> commandQueue)
    {
        this.commandQueue = commandQueue;
    }

    public int doWork() throws Exception
    {
        return commandQueue.drain(onCommandFunc);
    }

    private void onCommand(final SenderCommand command)
    {
        command.execute(this);
    }

    public void onConnect(final InetSocketAddress address)
    {
        try
        {
            final SocketChannel channel = SocketChannel.open();
            channel.connect(address);
            channel.configureBlocking(false);
            channels.add(channel);
        }
        catch (IOException e)
        {
            // TODO
            e.printStackTrace();
        }
    }

    public void onClose()
    {
        channels.forEach((socketChannel) -> {
            try
            {
                socketChannel.close();
            }
            catch (IOException e)
            {
                // TODO
                e.printStackTrace();
            }
        });
    }

    public String roleName()
    {
        return "Sender";
    }
}
