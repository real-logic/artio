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
package uk.co.real_logic.artio;

import io.aeron.Aeron;
import io.aeron.Counter;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;

import java.util.ArrayList;
import java.util.List;

import static org.agrona.concurrent.status.CountersManager.DEFAULT_TYPE_ID;

public class FixCounters implements AutoCloseable
{
    private final List<Counter> counters = new ArrayList<>();
    private final AtomicCounter failedInboundPublications;
    private final AtomicCounter failedOutboundPublications;
    private final AtomicCounter failedReplayPublications;
    private final Aeron aeron;

    FixCounters(final Aeron aeron)
    {
        this.aeron = aeron;
        failedInboundPublications = newCounter(DEFAULT_TYPE_ID, "Failed offer to inbound publication");
        failedOutboundPublications = newCounter(DEFAULT_TYPE_ID, "Failed offer to outbound publication");
        failedReplayPublications = newCounter(DEFAULT_TYPE_ID, "Failed offer to replay publication");
    }

    public AtomicCounter failedInboundPublications()
    {
        return failedInboundPublications;
    }

    public AtomicCounter failedOutboundPublications()
    {
        return failedOutboundPublications;
    }

    public AtomicCounter failedReplayPublications()
    {
        return failedReplayPublications;
    }

    public AtomicCounter messagesRead(final long connectionId, final String address)
    {
        return newCounter(DEFAULT_TYPE_ID, "Messages Read from " + address + " id = " + connectionId);
    }

    public AtomicCounter bytesInBuffer(final long connectionId, final String address)
    {
        return newCounter(DEFAULT_TYPE_ID, "Quarantined bytes for " + address + " id = " + connectionId);
    }

    public AtomicCounter invalidLibraryAttempts(final long connectionId, final String address)
    {
        return newCounter(DEFAULT_TYPE_ID, "Invalid Library Attempts for " + address + " id = " + connectionId);
    }

    public AtomicCounter sentMsgSeqNo(final long connectionId)
    {
        return newCounter(DEFAULT_TYPE_ID, "Last Sent MsgSeqNo for " + connectionId);
    }

    public AtomicCounter receivedMsgSeqNo(final long connectionId)
    {
        return newCounter(DEFAULT_TYPE_ID, "Last Received MsgSeqNo for " + connectionId);
    }

    private AtomicCounter newCounter(final int typeId, final String label)
    {
        final Counter counter = aeron.addCounter(typeId, label);
        counters.add(counter);
        return counter;
    }

    public void close()
    {
        Exceptions.closeAll(counters);
    }

}
