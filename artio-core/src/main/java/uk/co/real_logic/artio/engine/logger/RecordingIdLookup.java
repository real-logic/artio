/*
 * Copyright 2015-2022 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.archive.status.RecordingPos;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.archive.status.RecordingPos.NULL_RECORDING_ID;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

public class RecordingIdLookup
{
    private final Long2LongHashMap aeronSessionIdToRecordingId = new Long2LongHashMap(NULL_RECORDING_ID);
    private final IdleStrategy archiverIdleStrategy;
    private final CountersReader counters;

    public RecordingIdLookup(
        final IdleStrategy archiverIdleStrategy,
        final CountersReader counters)
    {
        this.archiverIdleStrategy = archiverIdleStrategy;
        this.counters = counters;
    }

    public long getRecordingId(final int aeronSessionId)
    {
        long recordingId = aeronSessionIdToRecordingId.get(aeronSessionId);

        while (recordingId == NULL_RECORDING_ID)
        {
            recordingId = checkRecordingId(aeronSessionId);

            archiverIdleStrategy.idle();
        }

        archiverIdleStrategy.reset();

        return recordingId;
    }

    long findRecordingId(final int aeronSessionId)
    {
        long recordingId = aeronSessionIdToRecordingId.get(aeronSessionId);

        if (recordingId == NULL_RECORDING_ID)
        {
            recordingId = checkRecordingId(aeronSessionId);
        }

        return recordingId;
    }

    private long checkRecordingId(final int aeronSessionId)
    {
        final int counterId = RecordingPos.findCounterIdBySession(counters, aeronSessionId);
        if (counterId == NULL_COUNTER_ID)
        {
            return NULL_RECORDING_ID;
        }

        final long recordingId = RecordingPos.getRecordingId(counters, counterId);
        if (recordingId != NULL_RECORDING_ID)
        {
            aeronSessionIdToRecordingId.put(aeronSessionId, recordingId);
        }
        return recordingId;
    }

}
