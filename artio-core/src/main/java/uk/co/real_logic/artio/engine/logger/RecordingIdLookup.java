/*
 * Copyright 2018 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.archive.status.RecordingPos;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.archive.status.RecordingPos.NULL_RECORDING_ID;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

public class RecordingIdLookup
{
    private final CountersReader counters;
    private final Long2LongHashMap aeronSessionIdToRecordingId = new Long2LongHashMap(NULL_RECORDING_ID);

    public RecordingIdLookup(final CountersReader counters)
    {
        this.counters = counters;
    }

    long getRecordingId(final int aeronSessionId)
    {
        long recordingId = aeronSessionIdToRecordingId.get(aeronSessionId);
        if (recordingId == NULL_RECORDING_ID)
        {
            int counterId;
            do
            {
                counterId = RecordingPos.findCounterIdBySession(counters, aeronSessionId);

                Thread.yield(); // TODO: properly idle.
            }
            while (counterId == NULL_COUNTER_ID);

            do
            {
                recordingId = RecordingPos.getRecordingId(counters, counterId);

                Thread.yield(); // TODO: properly idle.
            }
            while (recordingId == NULL_RECORDING_ID);

            aeronSessionIdToRecordingId.put(aeronSessionId, recordingId);
        }

        return recordingId;
    }
}
