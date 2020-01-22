/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.IdleStrategy;

import java.util.ArrayList;
import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.util.Comparator.comparingLong;

/**
 * Scan the archive for fix messages. Can be combined with predicates to create rich queries.
 *
 * @see FixMessageConsumer
 * @see FixMessagePredicate
 * @see FixMessagePredicates
 */
public class FixArchiveScanner implements AutoCloseable
{
    private final Aeron aeron;
    private final AeronArchive aeronArchive;
    private final IdleStrategy idleStrategy;

    public static class Context
    {
        private String aeronDirectoryName;
        private IdleStrategy idleStrategy;

        public Context()
        {
        }

        public Context aeronDirectoryName(final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            return this;
        }

        public String aeronDirectoryName()
        {
            return aeronDirectoryName;
        }

        public Context idleStrategy(final IdleStrategy idleStrategy)
        {
            this.idleStrategy = idleStrategy;
            return this;
        }

        public IdleStrategy idleStrategy()
        {
            return idleStrategy;
        }
    }

    public FixArchiveScanner(final Context context)
    {
        this.idleStrategy = context.idleStrategy();

        final Aeron.Context aeronContext = new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName());
        aeron = Aeron.connect(aeronContext);
        aeronArchive = AeronArchive.connect(new AeronArchive.Context().aeron(aeron).ownsAeronClient(true));
    }

    public void scan(
        final String aeronChannel,
        final int queryStreamId,
        final FixMessageConsumer handler,
        final boolean follow,
        final int archiveScannerStreamId)
    {
        final LogEntryHandler logEntryHandler = new LogEntryHandler(handler);
        final FragmentAssembler fragmentAssembler = new FragmentAssembler(logEntryHandler);

        final List<ArchiveLocation> archiveLocations = lookupArchiveLocations(aeronChannel, queryStreamId);

        try (Subscription replaySubscription = aeron.addSubscription(IPC_CHANNEL, archiveScannerStreamId))
        {
            archiveLocations.forEach(archiveLocation ->
            {
                final long recordingId = archiveLocation.recordingId;
                final boolean stillArchiving = archiveLocation.stopPosition == NULL_POSITION;

                final long stopPosition;
                final long length;
                if (stillArchiving)
                {
                    if (follow)
                    {
                        length = NULL_LENGTH;
                        stopPosition = NULL_POSITION;
                    }
                    else
                    {
                        stopPosition = aeronArchive.getRecordingPosition(recordingId);
                        length = stopPosition - archiveLocation.startPosition;
                    }
                }
                else
                {
                    stopPosition = archiveLocation.stopPosition;
                    length = stopPosition - archiveLocation.startPosition;
                }

                if (length != 0)
                {
                    final int sessionId = (int)aeronArchive.startReplay(
                        recordingId,
                        archiveLocation.startPosition,
                        length,
                        IPC_CHANNEL,
                        archiveScannerStreamId);

                    final Image image = lookupImage(replaySubscription, sessionId);

                    while (stopPosition == NULL_POSITION || image.position() < stopPosition)
                    {
                        idleStrategy.idle(image.poll(fragmentAssembler, 10));
                    }
                }
            });
        }
    }

    private Image lookupImage(final Subscription replaySubscription, final int sessionId)
    {
        Image image = null;

        while (image == null)
        {
            idleStrategy.idle();
            image = replaySubscription.imageBySessionId(sessionId);
        }
        idleStrategy.reset();

        return image;
    }

    private List<ArchiveLocation> lookupArchiveLocations(final String aeronChannel, final int queryStreamId)
    {
        final List<ArchiveLocation> archiveLocations = new ArrayList<>();
        aeronArchive.listRecordingsForUri(
            0,
            Integer.MAX_VALUE,
            aeronChannel,
            queryStreamId,
            (controlSessionId,
            correlationId,
            recordingId,
            startTimestamp,
            stopTimestamp,
            startPosition,
            stopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> archiveLocations.add(new ArchiveLocation(recordingId, startPosition, stopPosition)));

        // Any uncompleted recording is at the end
        archiveLocations.sort(comparingLong(ArchiveLocation::stopPosition).reversed());

        return archiveLocations;
    }

    static class ArchiveLocation
    {
        final long recordingId;
        final long startPosition;
        final long stopPosition;

        ArchiveLocation(final long recordingId, final long startPosition, final long stopPosition)
        {
            this.recordingId = recordingId;
            this.startPosition = startPosition;
            this.stopPosition = stopPosition;
        }

        public long stopPosition()
        {
            return stopPosition;
        }

        public String toString()
        {
            return "ArchiveReplayInfo{" +
                "recordingId=" + recordingId +
                ", startPosition=" + startPosition +
                ", stopPosition=" + stopPosition +
                '}';
        }
    }

    public void close()
    {
        aeronArchive.close();
    }
}
