/*
 * Copyright 2020 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.ReplayerCommand;
import uk.co.real_logic.artio.engine.logger.ReplayQuery;

import java.util.function.Predicate;

import static io.aeron.archive.client.AeronArchive.segmentFileBasePosition;

/**
 * PruneOperation is sent to the replayer in order to find the outbound replay query positions.
 * Then it get's sent to the Framer to query the inbound replay positions, then it delegates to
 * aeron archiver to prune the archive
 */
public class PruneOperation
    implements AdminCommand, ReplayerCommand, Reply<Long2LongHashMap>, RecordingDescriptorConsumer
{
    private final Predicate<AdminCommand> adminCommands;
    private final ReplayQuery outboundReplayQuery;
    private final ReplayQuery inboundReplayQuery;
    private final Long2LongHashMap newStartPositions = new Long2LongHashMap(Aeron.NULL_VALUE);
    private final Long2LongHashMap minimumPrunePositions;
    private final IdleStrategy idleStrategy;
    private final AeronArchive aeronArchive;

    private volatile State replyState;

    private Long2LongHashMap result;
    private Exception error;

    private long requestedNewStartPosition;
    private long segmentStartPosition;
    private long lowerBoundPrunePosition;

    public PruneOperation(final Exception error)
    {
        this(null, null, null, null, null, null);

        replyState = State.ERRORED;
        this.error = error;
    }

    public PruneOperation(
        final Long2LongHashMap minimumPrunePositions,
        final Predicate<AdminCommand> adminCommands,
        final ReplayQuery outboundReplayQuery,
        final ReplayQuery inboundReplayQuery,
        final IdleStrategy idleStrategy,
        final AeronArchive aeronArchive)
    {
        this.adminCommands = adminCommands;
        this.outboundReplayQuery = outboundReplayQuery;
        this.inboundReplayQuery = inboundReplayQuery;
        this.minimumPrunePositions = minimumPrunePositions;
        this.idleStrategy = idleStrategy;
        this.aeronArchive = aeronArchive;
        replyState = State.EXECUTING;
    }

    public Exception error()
    {
        return error;
    }

    public Long2LongHashMap resultIfPresent()
    {
        return result;
    }

    public State state()
    {
        return replyState;
    }

    // On Replayer Thread
    public void execute()
    {
        inboundReplayQuery.queryStartPositions(newStartPositions);

        moveToFramerThread();
    }

    private void moveToFramerThread()
    {
        while (!adminCommands.test(this))
        {
            idleStrategy.idle();
        }
        idleStrategy.reset();
    }

    // On Framer thread
    public void execute(final Framer framer)
    {
        outboundReplayQuery.queryStartPositions(newStartPositions);
        prune();
    }

    private void prune()
    {
        final Long2LongHashMap.EntryIterator it = newStartPositions.entrySet().iterator();
        while (it.hasNext())
        {
            it.next();

            final long recordingId = it.getLongKey();
            long newStartPosition = it.getLongValue();
            if (minimumPrunePositions != null)
            {
                final long requestedMinimumPosition = minimumPrunePositions.get(recordingId);
                if (requestedMinimumPosition != Aeron.NULL_VALUE)
                {
                    newStartPosition = Math.min(newStartPosition, requestedMinimumPosition);
                }
            }

            try
            {
                requestedNewStartPosition = newStartPosition;
                final int count = aeronArchive.listRecording(recordingId, this);
                if (count != 1)
                {
                    throw new IllegalStateException("Unable to list the recording: " + recordingId);
                }

                final long segmentStartPosition = this.segmentStartPosition;
                // Don't prune if you're < a segment away from the start of the stream.
                if (segmentStartPosition < lowerBoundPrunePosition)
                {
                    it.remove();
                }
                else
                {
                    aeronArchive.purgeSegments(recordingId, segmentStartPosition);
                    newStartPositions.put(recordingId, segmentStartPosition);
                }
            }
            catch (final Exception e)
            {
                e.printStackTrace();
                onPruneError(e, it);
                return;
            }
        }
        result = newStartPositions;
        replyState = State.COMPLETED;
    }

    public void onRecordingDescriptor(
        final long controlSessionId, final long correlationId, final long recordingId, final long startTimestamp,
        final long stopTimestamp, final long startPosition, final long stopPosition, final int initialTermId,
        final int segmentFileLength, final int termBufferLength, final int mtuLength, final int sessionId,
        final int streamId, final String strippedChannel, final String originalChannel, final String sourceIdentity)
    {
        segmentStartPosition = segmentFileBasePosition(
            startPosition, requestedNewStartPosition, termBufferLength, segmentFileLength);
        lowerBoundPrunePosition = segmentFileBasePosition(
            startPosition, startPosition, termBufferLength, segmentFileLength) + segmentFileLength;
    }

    private void onPruneError(final Exception e, final Long2LongHashMap.EntryIterator it)
    {
        it.remove();
        while (it.hasNext())
        {
            it.next();
            it.remove();
        }

        error = e;
        result = newStartPositions;
        replyState = State.ERRORED;
    }

    public String toString()
    {
        return "PruneOperation{" +
            "newStartPositions=" + newStartPositions +
            ", minimumPrunePositions=" + minimumPrunePositions +
            ", replyState=" + replyState +
            ", result=" + result +
            ", error=" + error +
            '}';
    }
}
