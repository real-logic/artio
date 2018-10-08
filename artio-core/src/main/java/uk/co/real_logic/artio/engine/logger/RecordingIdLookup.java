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
