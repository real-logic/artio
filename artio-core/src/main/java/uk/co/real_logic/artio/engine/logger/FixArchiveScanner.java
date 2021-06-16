/*
 * Copyright 2015-2021 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.ilink.ILinkMessageConsumer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static uk.co.real_logic.artio.engine.logger.FixMessageLogger.Configuration.DEFAULT_COMPACTION_SIZE;

/**
 * Scan the archive for fix messages. Can be combined with predicates to create rich queries.
 *
 * @see FixMessageConsumer
 * @see FixMessagePredicate
 * @see FixMessagePredicates
 */
public class FixArchiveScanner implements AutoCloseable
{
    private static final int FRAGMENT_LIMIT = 10;

    private static final ReversePositionComparator BY_REVERSE_POSITION = new ReversePositionComparator();

    private final Aeron aeron;
    private final AeronArchive aeronArchive;
    private final IdleStrategy idleStrategy;
    private final int compactionSize;

    public static class Configuration
    {
        private String aeronDirectoryName;
        private IdleStrategy idleStrategy;
        private int compactionSize = DEFAULT_COMPACTION_SIZE;

        public Configuration()
        {
        }

        public Configuration aeronDirectoryName(final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            return this;
        }

        public String aeronDirectoryName()
        {
            return aeronDirectoryName;
        }

        public Configuration idleStrategy(final IdleStrategy idleStrategy)
        {
            this.idleStrategy = idleStrategy;
            return this;
        }

        public IdleStrategy idleStrategy()
        {
            return idleStrategy;
        }

        public Configuration compactionSize(final int compactionSize)
        {
            this.compactionSize = compactionSize;
            return this;
        }

        public int compactionSize()
        {
            return compactionSize;
        }
    }

    public FixArchiveScanner(final Configuration configuration)
    {
        this.idleStrategy = configuration.idleStrategy();
        compactionSize = configuration.compactionSize;

        final Aeron.Context aeronContext = new Aeron.Context().aeronDirectoryName(configuration.aeronDirectoryName());
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
        scan(aeronChannel, queryStreamId, handler, null, follow, archiveScannerStreamId);
    }

    public void scan(
        final String aeronChannel,
        final int queryStreamId,
        final FixMessageConsumer fixHandler,
        final ILinkMessageConsumer iLinkHandler,
        final boolean follow,
        final int archiveScannerStreamId)
    {
        final IntHashSet queryStreamIds = new IntHashSet();
        queryStreamIds.add(queryStreamId);
        scan(aeronChannel, queryStreamIds, fixHandler, iLinkHandler, follow, archiveScannerStreamId);
    }

    public void scan(
        final String aeronChannel,
        final IntHashSet queryStreamIds,
        final FixMessageConsumer fixHandler,
        final ILinkMessageConsumer iLinkHandler,
        final boolean follow,
        final int archiveScannerStreamId)
    {
        try (Subscription replaySubscription = aeron.addSubscription(IPC_CHANNEL, archiveScannerStreamId))
        {
            final RecordingPoller[] pollers = queryStreamIds
                .stream()
                .map(id -> makePoller(id, replaySubscription, follow, aeronChannel))
                .toArray(RecordingPoller[]::new);

            final StreamTimestampZipper timestampZipper = new StreamTimestampZipper(
                fixHandler, iLinkHandler, compactionSize, pollers);

            while (true)
            {
                final int received = timestampZipper.poll();

                // Don't need to do this check in follow mode as we're just going to keep running and not terminate.
                if (0 == received && !follow) // lgtm [java/constant-loop-condition]
                {
                    if (checkCompletion(pollers))
                    {
                        timestampZipper.onClose();
                        idleStrategy.reset();
                        return;
                    }
                }

                idleStrategy.idle(received);
            }
        }
    }

    private boolean checkCompletion(final RecordingPoller[] pollers)
    {
        for (final RecordingPoller poller : pollers)
        {
            if (!poller.isComplete())
            {
                return false;
            }
        }

        return true;
    }

    private RecordingPoller makePoller(
        final int queryStreamId,
        final Subscription replaySubscription,
        final boolean follow,
        final String aeronChannel)
    {
        final List<ArchiveLocation> archiveLocations = new ArrayList<>();

        aeronArchive.listRecordings(0,
            Integer.MAX_VALUE,
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
            sourceIdentity) ->
            {
                final ChannelUri uri = ChannelUri.parse(strippedChannel);
                uri.remove(CommonContext.SESSION_ID_PARAM_NAME);
                final String comparableChannel = uri.toString();

                if (streamId == queryStreamId && comparableChannel.contains(aeronChannel))
                {
                    archiveLocations.add(new ArchiveLocation(recordingId, startPosition, stopPosition));
                }
            });

        if (!follow)
        {
            for (final ArchiveLocation location : archiveLocations)
            {
                if (location.stopPosition == NULL_POSITION)
                {
                    location.stopPosition = aeronArchive.getRecordingPosition(location.recordingId);
                }
            }
        }

        archiveLocations.sort(BY_REVERSE_POSITION);

        return new RecordingPoller(replaySubscription, queryStreamId, archiveLocations);
    }

    static class ReversePositionComparator implements Comparator<ArchiveLocation>
    {
        public int compare(final ArchiveLocation archiveLocation1, final ArchiveLocation archiveLocation2)
        {
            return -1 * Long.compare(getStopPosition(archiveLocation1), getStopPosition(archiveLocation2));
        }

        long getStopPosition(final ArchiveLocation archiveLocation)
        {
            final long stopPosition = archiveLocation.stopPosition;
            return stopPosition == NULL_POSITION ? Long.MAX_VALUE : stopPosition;
        }
    }

    static class ArchiveLocation
    {
        final long recordingId;
        final long startPosition;

        long stopPosition;

        ArchiveLocation(
            final long recordingId, final long startPosition, final long stopPosition)
        {
            this.recordingId = recordingId;
            this.startPosition = startPosition;
            this.stopPosition = stopPosition;
        }

        public long stopPosition()
        {
            return stopPosition;
        }

        public long length()
        {
            return startPosition == NULL_POSITION ? NULL_POSITION : stopPosition - startPosition;
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

    class RecordingPoller implements StreamTimestampZipper.Poller
    {
        private final List<ArchiveLocation> archiveLocations;
        private final Subscription replaySubscription;
        private final int originalStreamId;

        long stopPosition;
        Image image;

        RecordingPoller(
            final Subscription replaySubscription,
            final int originalStreamId,
            final List<ArchiveLocation> archiveLocations)
        {
            this.replaySubscription = replaySubscription;
            this.originalStreamId = originalStreamId;
            this.archiveLocations = archiveLocations;
        }

        boolean isComplete()
        {
            return stopPosition != NULL_POSITION && image == null && archiveLocations.isEmpty();
        }

        public int poll(final FragmentAssembler fragmentAssembler)
        {
            if (image == null)
            {
                if (archiveLocations.isEmpty())
                {
                    return 0;
                }

                final ArchiveLocation archiveLocation = archiveLocations.remove(archiveLocations.size() - 1);

                if (archiveLocation.length() != 0)
                {
                    final int sessionId = (int)aeronArchive.startReplay(
                        archiveLocation.recordingId,
                        archiveLocation.startPosition,
                        archiveLocation.length(),
                        IPC_CHANNEL,
                        replaySubscription.streamId());

                    image = lookupImage(sessionId);
                    stopPosition = archiveLocation.stopPosition;
                }

                return 1;
            }
            else
            {
                if (image.position() >= stopPosition)
                {
                    image = null;
                    return 1;
                }
                else
                {
                    return image.poll(fragmentAssembler, FRAGMENT_LIMIT);
                }
            }
        }

        public int streamId()
        {
            return originalStreamId;
        }

        private Image lookupImage(final int sessionId)
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

        public void close()
        {
            // don't own replay subscription so no need to close it.
        }
    }

    public void close()
    {
        aeronArchive.close();
    }
}
