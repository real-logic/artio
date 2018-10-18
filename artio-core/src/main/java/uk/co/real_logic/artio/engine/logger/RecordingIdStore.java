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
import org.agrona.concurrent.AgentInvoker;

import static uk.co.real_logic.artio.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.GatewayProcess.OUTBOUND_LIBRARY_STREAM;

public class RecordingIdStore implements AutoCloseable
{
    private static final int FRAGMENT_COUNT_LIMIT = 10;

    private final RecordingIdLookup inboundLookup;
    private final RecordingIdLookup outboundLookup;
    private final Subscription subscription;
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
            if (channel.equals(requiredChannel))
            {
                inboundLookup.onStart(recordingId, sessionId, streamId);
                outboundLookup.onStart(recordingId, sessionId, streamId);
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

    public RecordingIdStore(
        final Aeron aeron,
        final String requiredChannel,
        final AgentInvoker conductorAgentInvoker)
    {
        subscription = aeron.addSubscription(
            AeronArchive.Configuration.recordingEventsChannel(),
            AeronArchive.Configuration.recordingEventsStreamId());
        this.requiredChannel = requiredChannel;

        recordingEventsAdapter = new RecordingEventsAdapter(
            recordingEventsListener, subscription, FRAGMENT_COUNT_LIMIT);

        inboundLookup = new RecordingIdLookup(INBOUND_LIBRARY_STREAM, this);
        outboundLookup = new RecordingIdLookup(OUTBOUND_LIBRARY_STREAM, this);

        // Wait for the subscription setup to ensure that we receive onStart events of all
        // recording started after this constructor.
        while (!subscription.isConnected())
        {
            poll();

            if (conductorAgentInvoker != null)
            {
                conductorAgentInvoker.invoke();
            }

            Thread.yield(); // TODO: properly idle.
        }
    }

    public RecordingIdLookup inboundLookup()
    {
        return inboundLookup;
    }

    public RecordingIdLookup outboundLookup()
    {
        return outboundLookup;
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
