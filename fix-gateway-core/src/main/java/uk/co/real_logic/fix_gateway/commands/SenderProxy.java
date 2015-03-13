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

import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.framer.SenderEndPoint;

import java.util.Queue;

public class SenderProxy
{
    private final Queue<SenderCommand> commandQueue;

    public SenderProxy(final Queue<SenderCommand> commandQueue)
    {
        this.commandQueue = commandQueue;
    }

    public void connect(final SessionConfiguration configuration)
    {
        offer(new Connect(configuration));
    }

    public void newAcceptedConnection(final SenderEndPoint senderEndPoint)
    {
        offer(new NewAcceptedConnection(senderEndPoint));
    }

    private void offer(final SenderCommand command)
    {
        // TODO: decide on retry/backoff strategy
        commandQueue.offer(command);
    }
}
