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
package uk.co.real_logic.artio;

import io.aeron.Aeron;
import io.aeron.Counter;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.session.Session;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

import static uk.co.real_logic.artio.FixCounters.FixCountersId.*;
import static uk.co.real_logic.artio.GatewayProcess.NO_CONNECTION_ID;

public class FixCounters implements AutoCloseable
{
    private static final int MINIMUM_ARTIO_TYPE_ID = 10_000;

    public enum FixCountersId
    {
        FAILED_INBOUND_TYPE_ID(MINIMUM_ARTIO_TYPE_ID),
        FAILED_OUTBOUND_TYPE_ID(10_001),
        FAILED_REPLAY_TYPE_ID(10_002),
        MESSAGES_READ_TYPE_ID(10_003),
        BYTES_IN_BUFFER_TYPE_ID(10_004),
        INVALID_LIBRARY_ATTEMPTS_TYPE_ID(10_005),
        SENT_MSG_SEQ_NO_TYPE_ID(10_006),
        RECV_MSG_SEQ_NO_TYPE_ID(10_007),
        CURRENT_REPLAY_COUNT_TYPE_ID(10_008),
        NEGATIVE_TIMESTAMP_TYPE_ID(10_009),
        FAILED_ADMIN_TYPE_ID(10_010),
        FAILED_ADMIN_REPLY_TYPE_ID(10_011);

        final int id;

        FixCountersId(final int id)
        {
            this.id = id;
        }

        public int id()
        {
            return id;
        }
    }

    private final List<Counter> counters = new CopyOnWriteArrayList<>();
    private final AtomicCounter failedInboundPublications;
    private final AtomicCounter failedOutboundPublications;
    private final AtomicCounter failedReplayPublications;
    private final AtomicCounter failedAdminReplyPublications;
    private final AtomicCounter currentReplayCount;
    private final AtomicCounter negativeTimestamps;
    private final Aeron aeron;

    public static IntHashSet lookupCounterIds(
        final FixCountersId counterTypeId, final CountersReader countersReader)
    {
        return lookupCounterIds(counterTypeId, countersReader, label -> true);
    }

    public static IntHashSet lookupCounterIds(
        final FixCountersId counterTypeId, final CountersReader countersReader, final Predicate<String> labelCheck)
    {
        final int requiredTypeId = counterTypeId.id();
        final IntHashSet counterIds = new IntHashSet();
        countersReader.forEach((counterId, typeId, keyBuffer, label) ->
        {
            if (typeId == requiredTypeId && labelCheck.test(label))
            {
                counterIds.add(counterId);
            }
        });
        return counterIds;
    }

    FixCounters(final Aeron aeron, final boolean isEngine, final int libraryId)
    {
        this.aeron = aeron;
        aeron.addUnavailableCounterHandler((countersReader, registrationId, counterId) ->
            counters.removeIf(counter -> counter.id() == counterId));
        failedInboundPublications = newCounter(FAILED_INBOUND_TYPE_ID.id(),
                "Failed offer to inbound publication " + libraryId);
        failedOutboundPublications = newCounter(FAILED_OUTBOUND_TYPE_ID.id(),
                "Failed offer to outbound publication " + libraryId);
        failedReplayPublications = newCounter(FAILED_REPLAY_TYPE_ID.id(),
                "Failed offer to replay publication " + libraryId);
        failedAdminReplyPublications = newCounter(FAILED_ADMIN_REPLY_TYPE_ID.id(),
            "Failed offer to admin reply publication " + libraryId);

        negativeTimestamps = newCounter(NEGATIVE_TIMESTAMP_TYPE_ID.id(), "negative timestamps " + libraryId);

        if (isEngine)
        {
            currentReplayCount = newCounter(FixCountersId.CURRENT_REPLAY_COUNT_TYPE_ID.id(),
                "Current Replay Count");
        }
        else
        {
            currentReplayCount = null;
        }
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

    public AtomicCounter failedAdminReplyPublications()
    {
        return failedAdminReplyPublications;
    }

    public AtomicCounter currentReplayCount()
    {
        return currentReplayCount;
    }

    public AtomicCounter negativeTimestamps()
    {
        return negativeTimestamps;
    }

    public AtomicCounter messagesRead(final long connectionId, final String address)
    {
        return newCounter(FixCountersId.MESSAGES_READ_TYPE_ID.id(),
                "Messages Read from " + address + " id = " + connectionId);
    }

    public AtomicCounter bytesInBuffer(final long connectionId, final String address)
    {
        return newCounter(FixCountersId.BYTES_IN_BUFFER_TYPE_ID.id(),
                "Quarantined bytes for " + address + " id = " + connectionId);
    }

    public AtomicCounter invalidLibraryAttempts(final long connectionId, final String address)
    {
        return newCounter(FixCountersId.INVALID_LIBRARY_ATTEMPTS_TYPE_ID.id(),
                "Invalid Library Attempts for " + address + " id = " + connectionId);
    }

    public AtomicCounter sentMsgSeqNo(final long connectionId, final long sessionId)
    {
        return newCounter(
            FixCountersId.SENT_MSG_SEQ_NO_TYPE_ID.id(), msgSeqNoLabel("Sent", connectionId, sessionId));
    }

    public AtomicCounter receivedMsgSeqNo(final long connectionId, final long sessionId)
    {
        return newCounter(
            FixCountersId.RECV_MSG_SEQ_NO_TYPE_ID.id(), msgSeqNoLabel("Received", connectionId, sessionId));
    }

    private String msgSeqNoLabel(final String type, final long connectionId, final long sessionId)
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("Last ");
        sb.append(type);
        sb.append(" MsgSeqNo ");

        final boolean hasConnectionId = connectionId != NO_CONNECTION_ID;
        if (hasConnectionId)
        {
            sb.append("connId=");
            sb.append(connectionId);
        }

        if (sessionId != Session.UNKNOWN)
        {
            if (hasConnectionId)
            {
                sb.append(",");
            }
            sb.append("sessId=");
            sb.append(sessionId);
        }

        return sb.toString();
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
