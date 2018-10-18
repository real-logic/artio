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

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.archive.client.RecordingEventsListener;
import org.agrona.collections.Long2LongHashMap;

import static io.aeron.archive.status.RecordingPos.NULL_RECORDING_ID;

public class RecordingIdLookup implements AutoCloseable
{
    private static final int FRAGMENT_COUNT_LIMIT = 10;

    private final Long2LongHashMap aeronSessionIdToRecordingId = new Long2LongHashMap(NULL_RECORDING_ID);
    private final Subscription subscription;
    private final int requiredStreamId;
    private final String requiredChannel;
    private final RecordingEventsAdapter recordingEventsAdapter;
    // TODO: remove allocation of channel and sourceidentity strings
    // TODO: when does recording id change? Is it range limited.
    private final RecordingEventsListener recordingEventsListener = new RecordingEventsListener()
    {
        @Override
        public void onStart(
            final long recordingId,
            final long startPosition,
            final int sessionId,
            final int streamId,
            final String channel,
            final String sourceIdentity)
        {
            if (streamId == requiredStreamId && channel.equals(requiredChannel))
            {
                // System.out.printf("Mapping %d -> %d%n", sessionId, recordingId);
                aeronSessionIdToRecordingId.put(sessionId, recordingId);
            }
        }

        @Override
        public void onProgress(final long recordingId, final long startPosition, final long position)
        {
        }

        @Override
        public void onStop(final long recordingId, final long startPosition, final long stopPosition)
        {
        }
    };

    public RecordingIdLookup(final Aeron aeron, final String requiredChannel, final int requiredStreamId)
    {
        subscription = aeron.addSubscription(
            AeronArchive.Configuration.recordingEventsChannel(),
            AeronArchive.Configuration.recordingEventsStreamId());
        this.requiredStreamId = requiredStreamId;
        this.requiredChannel = requiredChannel;
        recordingEventsAdapter = new RecordingEventsAdapter(
            recordingEventsListener, subscription, FRAGMENT_COUNT_LIMIT);

        // Wait for the subscription setup to ensure that we receive onStart events of all
        // recording started after this constructor.
        while (!subscription.isConnected())
        {
            poll();

            Thread.yield(); // TODO: properly idle.
        }
    }

    long getRecordingId(final int aeronSessionId)
    {
        while (true)
        {
            final long recordingId = aeronSessionIdToRecordingId.get(aeronSessionId);
            if (recordingId == NULL_RECORDING_ID)
            {
                poll();
            }
            else
            {
                return recordingId;
            }

            Thread.yield(); // TODO: properly idle.
        }
    }

    int poll()
    {
        return recordingEventsAdapter.poll();
    }

    @Override
    public void close()
    {
        subscription.close();
    }
}
