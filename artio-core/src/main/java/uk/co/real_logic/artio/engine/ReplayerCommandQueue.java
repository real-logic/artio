/*
 * Copyright 2020 Adaptive Financial Consulting Limited.
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
package uk.co.real_logic.artio.engine;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.function.Consumer;

public class ReplayerCommandQueue
{
    private static final int CAPACITY = 64;

    // Framer state
    private final IdleStrategy framerIdleStrategy;

    // Written on Framer, Read on Indexer
    private final OneToOneConcurrentArrayQueue<ReplayerCommand> queue
        = new OneToOneConcurrentArrayQueue<>(CAPACITY);
    private final Consumer<ReplayerCommand> onReplayerCommand = this::onReplayerCommand;

    public ReplayerCommandQueue(final IdleStrategy framerIdleStrategy)
    {
        this.framerIdleStrategy = framerIdleStrategy;
    }

    public void enqueue(final ReplayerCommand command)
    {
        while (!offer(command))
        {
            framerIdleStrategy.idle();
        }
        framerIdleStrategy.reset();
    }

    public boolean offer(final ReplayerCommand command)
    {
        return queue.offer(command);
    }

    public int poll()
    {
        return queue.drain(onReplayerCommand, CAPACITY);
    }

    private void onReplayerCommand(final ReplayerCommand command)
    {
        command.execute();
    }
}
