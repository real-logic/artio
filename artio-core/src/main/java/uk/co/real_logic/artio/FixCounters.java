/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.artio;

import io.aeron.Aeron;
import io.aeron.Counter;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;

import java.util.ArrayList;
import java.util.List;

public class FixCounters implements AutoCloseable
{

    private static final int MINIMUM_ARTIO_TYPE_ID = 10_000;
    private static final int FAILED_INBOUND_TYPE_ID = MINIMUM_ARTIO_TYPE_ID;
    private static final int FAILED_OUTBOUND_TYPE_ID = 10_001;
    private static final int FAILED_REPLAY_TYPE_ID = 10_002;
    private static final int MESSAGES_READ_TYPE_ID = 10_003;
    private static final int BYTES_IN_BUFFER_TYPE_ID = 10_004;
    private static final int INVALID_LIBRARY_ATTEMPTS_TYPE_ID = 10_005;
    private static final int SENT_MSG_SEQ_NO_TYPE_ID = 10_006;
    private static final int RECV_MSG_SEQ_NO_TYPE_ID = 10_007;

    private final List<Counter> counters = new ArrayList<>();
    private final AtomicCounter failedInboundPublications;
    private final AtomicCounter failedOutboundPublications;
    private final AtomicCounter failedReplayPublications;
    private final Aeron aeron;

    FixCounters(final Aeron aeron)
    {
        this.aeron = aeron;
        failedInboundPublications = newCounter(FAILED_INBOUND_TYPE_ID, "Failed offer to inbound publication");
        failedOutboundPublications = newCounter(FAILED_OUTBOUND_TYPE_ID, "Failed offer to outbound publication");
        failedReplayPublications = newCounter(FAILED_REPLAY_TYPE_ID, "Failed offer to replay publication");
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
        return newCounter(MESSAGES_READ_TYPE_ID, "Messages Read from " + address + " id = " + connectionId);
    }

    public AtomicCounter bytesInBuffer(final long connectionId, final String address)
    {
        return newCounter(BYTES_IN_BUFFER_TYPE_ID, "Quarantined bytes for " + address + " id = " + connectionId);
    }

    public AtomicCounter invalidLibraryAttempts(final long connectionId, final String address)
    {
        return newCounter(INVALID_LIBRARY_ATTEMPTS_TYPE_ID,
            "Invalid Library Attempts for " + address + " id = " + connectionId);
    }

    public AtomicCounter sentMsgSeqNo(final long connectionId)
    {
        return newCounter(SENT_MSG_SEQ_NO_TYPE_ID, "Last Sent MsgSeqNo for " + connectionId);
    }

    public AtomicCounter receivedMsgSeqNo(final long connectionId)
    {
        return newCounter(RECV_MSG_SEQ_NO_TYPE_ID, "Last Received MsgSeqNo for " + connectionId);
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
