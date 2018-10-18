/*
 * Copyright 2015-2018 Real Logic Ltd.
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
package uk.co.real_logic.artio.engine;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.CloseHelper;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.IntHashSet.IntIterator;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.status.CountersReader;
import uk.co.real_logic.artio.engine.logger.RecordingIdLookup;

import java.util.ArrayList;
import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.archive.codecs.SourceLocation.REMOTE;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static uk.co.real_logic.artio.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.GatewayProcess.OUTBOUND_LIBRARY_STREAM;

/**
 * Not thread safe.
 */
public class RecordingCoordinator implements AutoCloseable
{
    private final IntHashSet trackedSessionIds = new IntHashSet();
    private final AeronArchive archive;
    private final String channel;
    private final CountersReader counters;
    private final EngineConfiguration configuration;

    private Long2LongHashMap inboundAeronSessionIdToCompletionPosition;
    private Long2LongHashMap outboundAeronSessionIdToCompletionPosition;

    // TODO: these are always used on the same thread - can I combine them and reduce the
    // number of subscriptions?
    private final RecordingIdLookup inboundRecordingIdLookup;
    private final RecordingIdLookup outboundRecordingIdLookup;

    private boolean closed = false;

    RecordingCoordinator(final AeronArchive archive, final EngineConfiguration configuration)
    {
        this.archive = archive;
        this.configuration = configuration;
        this.channel = configuration.libraryAeronChannel();
        if (configuration.logAnyMessages())
        {
            final Aeron aeron = archive.context().aeron();
            counters = aeron.countersReader();

            if (configuration.logInboundMessages())
            {
                inboundRecordingIdLookup = new RecordingIdLookup(aeron, channel, INBOUND_LIBRARY_STREAM);

                // Inbound we're writing from the Framer thread, always local
                archive.startRecording(channel, INBOUND_LIBRARY_STREAM, LOCAL);
            }
            else
            {
                inboundRecordingIdLookup = null;
            }

            if (configuration.logOutboundMessages())
            {
                outboundRecordingIdLookup = new RecordingIdLookup(aeron, channel, OUTBOUND_LIBRARY_STREAM);

                // Outbound libraries might be on an IPC box.
                final SourceLocation location = channel.equals(IPC_CHANNEL) ? LOCAL : REMOTE;
                archive.startRecording(channel, OUTBOUND_LIBRARY_STREAM, location);
            }
            else
            {
                outboundRecordingIdLookup = null;
            }
        }
        else
        {
            counters = null;
            inboundRecordingIdLookup = null;
            outboundRecordingIdLookup = null;
        }
    }

    // Only called on single threaded engine startup
    public void track(final Publication publication)
    {
        final int streamId = publication.streamId();
        if ((streamId == OUTBOUND_LIBRARY_STREAM && configuration.logOutboundMessages()) ||
            (streamId == INBOUND_LIBRARY_STREAM && configuration.logInboundMessages()))
        {
            trackedSessionIds.add(publication.sessionId());
        }
    }

    // Only called on single threaded engine startup
    void awaitReady()
    {
        while (!trackedSessionIds.isEmpty())
        {
            final IntIterator sessionIdIterator = trackedSessionIds.iterator();
            while (sessionIdIterator.hasNext())
            {
                final int sessionId = sessionIdIterator.nextValue();
                if (hasRecordingStarted(sessionId))
                {
                    sessionIdIterator.remove();
                }
            }

            Thread.yield();
        }
    }

    // Called only on Framer.quiesce(), uses shutdown order
    public void completionPositions(
        final Long2LongHashMap inboundAeronSessionIdToCompletionPosition,
        final Long2LongHashMap outboundAeronSessionIdToCompletionPosition)
    {
        this.inboundAeronSessionIdToCompletionPosition = inboundAeronSessionIdToCompletionPosition;
        this.outboundAeronSessionIdToCompletionPosition = outboundAeronSessionIdToCompletionPosition;
    }

    // Must be called after the framer has shutdown, uses shutdown order
    @Override
    public void close()
    {
        if (!closed)
        {
            // awaitRecordingsCompletion();
            shutdownArchiver();
            closed = true;
        }
    }

    private void awaitRecordingsCompletion()
    {
        if (configuration.logInboundMessages())
        {
            awaitRecordingsCompletion(inboundAeronSessionIdToCompletionPosition);
        }

        if (configuration.logOutboundMessages())
        {
            awaitRecordingsCompletion(outboundAeronSessionIdToCompletionPosition);
        }
    }

    private void awaitRecordingsCompletion(final Long2LongHashMap aeronSessionIdToCompletionPosition)
    {
        if (aeronSessionIdToCompletionPosition == null)
        {
            throw new IllegalStateException(
                "Unknown completionPositions when shutting down the RecordingCoordinator");
        }

        final List<CompletingRecording> completingRecordings = new ArrayList<>();
        aeronSessionIdToCompletionPosition.longForEach((sessionId, completionPosition) ->
            completingRecordings.add(new CompletingRecording((int)sessionId, completionPosition)));

        while (!completingRecordings.isEmpty())
        {
            completingRecordings.removeIf(CompletingRecording::hasRecordingCompleted);

            Thread.yield();
        }
    }

    private void shutdownArchiver()
    {
        if (configuration.logInboundMessages())
        {
            System.out.println("stopping inbound");
            archive.stopRecording(channel, INBOUND_LIBRARY_STREAM);

            CloseHelper.close(inboundRecordingIdLookup);
        }

        if (configuration.logOutboundMessages())
        {
            System.out.println("stopping outbound");
            archive.stopRecording(channel, OUTBOUND_LIBRARY_STREAM);

            CloseHelper.close(outboundRecordingIdLookup);
        }

        if (configuration.logAnyMessages())
        {
            archive.close();
        }
    }

    private boolean hasRecordingStarted(final int sessionId)
    {
        return RecordingPos.findCounterIdBySession(counters, sessionId) != NULL_COUNTER_ID;
    }

    private class CompletingRecording
    {
        private final long completedPosition;
        private final long recordingId;
        private final int counterId;

        CompletingRecording(final int sessionId, final long completedPosition)
        {
            this.completedPosition = completedPosition;

            counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
            recordingId = RecordingPos.getRecordingId(counters, counterId);
        }

        boolean hasRecordingCompleted()
        {
            final long recordedPosition = counters.getCounterValue(counterId);
            if (recordedPosition >= completedPosition)
            {
                return true;
            }

            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new IllegalStateException("recording has stopped unexpectedly: " + recordingId);
            }

            return false;
        }
    }

    public RecordingIdLookup inboundRecordingIdLookup()
    {
        return inboundRecordingIdLookup;
    }

    public RecordingIdLookup outboundRecordingIdLookup()
    {
        return outboundRecordingIdLookup;
    }
}
