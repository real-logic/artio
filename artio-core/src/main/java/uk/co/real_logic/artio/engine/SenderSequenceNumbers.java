/*
 * Copyright 2014-2018 Real Logic Limited.
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

import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

import java.util.function.Consumer;

/**
 * For publishing the last sent sequence number to the replay system.
 */
public class SenderSequenceNumbers
{
    public static final int UNKNOWN_SESSION = -1;

    private static final int CAPACITY = 10;

    // Framer state
    private final IdleStrategy framerIdleStrategy;

    // Written on Framer, Read on Indexer
    private final OneToOneConcurrentArrayQueue<SenderSequenceNumber> queue
        = new OneToOneConcurrentArrayQueue<>(CAPACITY);

    // Indexer State
    private final Long2ObjectHashMap<SenderSequenceNumber> connectionIdToSequencePosition
        = new Long2ObjectHashMap<>();
    private final Consumer<SenderSequenceNumber> onSenderSequenceNumberFunc
        = this::onSenderSequenceNumber;

    public SenderSequenceNumbers(final IdleStrategy framerIdleStrategy)
    {
        this.framerIdleStrategy = framerIdleStrategy;
    }

    // Called on Framer Thread
    public SenderSequenceNumber onNewSender(final long connectionId)
    {
        final SenderSequenceNumber position = new SenderSequenceNumber(connectionId, this);
        enqueue(position);
        return position;
    }

    // Called on Framer Thread
    void onSenderClosed(final SenderSequenceNumber senderSequenceNumber)
    {
        enqueue(senderSequenceNumber);
    }

    // We receive the object to either add or remove it.
    private void enqueue(final SenderSequenceNumber senderSequenceNumber)
    {
        while (!queue.offer(senderSequenceNumber))
        {
            framerIdleStrategy.idle();
        }
        framerIdleStrategy.reset();
    }

    // Called on Indexer Thread
    public int poll()
    {
        return queue.drain(onSenderSequenceNumberFunc, CAPACITY);
    }

    // Called on Indexer Thread
    public int lastSentSequenceNumber(final long connectionId)
    {
        final SenderSequenceNumber senderSequenceNumber = connectionIdToSequencePosition.get(connectionId);
        if (senderSequenceNumber == null)
        {
            return UNKNOWN_SESSION;
        }

        return senderSequenceNumber.lastSentSequenceNumber();
    }

    // Called on Indexer Thread
    private void onSenderSequenceNumber(final SenderSequenceNumber senderSequenceNumber)
    {
        final long connectionId = senderSequenceNumber.connectionId();
        if (connectionIdToSequencePosition.remove(connectionId) == null)
        {
            connectionIdToSequencePosition.put(connectionId, senderSequenceNumber);
        }
    }
}
