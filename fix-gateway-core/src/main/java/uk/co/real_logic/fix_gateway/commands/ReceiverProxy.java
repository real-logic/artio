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
package uk.co.real_logic.fix_gateway.commands;

import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.framer.ReceiverEndPoint;

import java.util.Queue;

public class ReceiverProxy
{
    private final Queue<ReceiverCommand> commandQueue;
    private final AtomicCounter fails;
    private final IdleStrategy idleStrategy;

    public ReceiverProxy(
        final Queue<ReceiverCommand> commandQueue, final AtomicCounter fails, final IdleStrategy idleStrategy)
    {
        this.commandQueue = commandQueue;
        this.fails = fails;
        this.idleStrategy = idleStrategy;
    }

    public void newInitiatedConnection(final ReceiverEndPoint receiverEndPoint)
    {
        offer(new NewInitiatedConnection(receiverEndPoint));
    }

    public void disconnect(final long connectionId)
    {
        offer(new ReceiverDisconnect(connectionId));
    }

    private void offer(final ReceiverCommand command)
    {
        while (!commandQueue.offer(command))
        {
            fails.increment();
            idleStrategy.idle(1);
        }
    }
}
