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

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.fixp.FixPMessageConsumer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static uk.co.real_logic.artio.LogTag.ARCHIVE_SCAN;
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
    public static final int DEFAULT_FRAGMENT_LIMIT = 10000;

    static final boolean DEBUG_LOG_ARCHIVE_SCAN = DebugLogger.isEnabled(ARCHIVE_SCAN);

    private final Aeron aeron;
    private final AeronArchive aeronArchive;
    private final IdleStrategy idleStrategy;
    private final int compactionSize;
    private final int fragmentLimit;

    private final String logFileDir;

    private final Long2ObjectHashMap<TimeIndexReader> streamIdToInboundTimeIndex = new Long2ObjectHashMap<>();

    public static class Configuration
    {
        private int fragmentLimit = DEFAULT_FRAGMENT_LIMIT;
        private String aeronDirectoryName;
        private IdleStrategy idleStrategy;
        private int compactionSize = DEFAULT_COMPACTION_SIZE;
        private String logFileDir;
        private boolean enableIndexScan;
        private AeronArchive.Context archiveContext;

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

        /**
         * Sets the fragment limit for polling different images when archive scanning.
         *
         * @param fragmentLimit the fragment limit
         * @return this
         */
        public Configuration fragmentLimit(final int fragmentLimit)
        {
            this.fragmentLimit = fragmentLimit;
            return this;
        }

        public int fragmentLimit()
        {
            return fragmentLimit;
        }

        /**
         * Sets the logFileDir used by your {@link EngineConfiguration}. This configuration option isn't required, it
         * allows faster FixArchiveScanner operations for predicates where you're searching by time by using the
         * {@link FixMessagePredicates#to(long)} or {@link FixMessagePredicates#from(long)} predicates.
         * Setting this configuration option automatically enables index scanning.
         *
         * @param logFileDir the logFileDir configured in your {@link EngineConfiguration}.
         * @return this
         */
        public Configuration logFileDir(final String logFileDir)
        {
            this.logFileDir = logFileDir;
            this.enableIndexScan = true;
            return this;
        }

        public String logFileDir()
        {
            return logFileDir;
        }

        /**
         * Enables or disables index scanning. If set to true, a {@link #logFileDir(String)} is required.
         *
         * @param enableIndexScan true to enable time based index scanning, false otherwise.
         * @return this
         */
        public Configuration enableIndexScan(final boolean enableIndexScan)
        {
            this.enableIndexScan = enableIndexScan;
            return this;
        }

        public boolean enableIndexScan()
        {
            return enableIndexScan;
        }

        /**
         * Sets the context to be used to create the Aeron Archiver that this backs onto.
         *
         * NB: this archiver will be given ownership of an Aeron instance, so a custom Aeron instance
         * shouldn't be set on the archive context.
         *
         * @param archiveContext the context to use to create the aeron archiver.
         * @return this
         */
        public Configuration archiveContext(final AeronArchive.Context archiveContext)
        {
            this.archiveContext = archiveContext;
            return this;
        }

        private void conclude()
        {
            if (enableIndexScan && logFileDir == null)
            {
                throw new IllegalArgumentException("Please configure a logFileDir if you want to enable index scan");
            }
        }
    }

    public FixArchiveScanner(final Configuration configuration)
    {
        configuration.conclude();

        this.idleStrategy = configuration.idleStrategy();
        compactionSize = configuration.compactionSize;
        fragmentLimit = configuration.fragmentLimit;

        final Aeron.Context aeronContext = new Aeron.Context().aeronDirectoryName(configuration.aeronDirectoryName());
        aeron = Aeron.connect(aeronContext);

        AeronArchive.Context archiveContext = configuration.archiveContext;
        if (archiveContext == null)
        {
            archiveContext = new AeronArchive.Context();
        }
        // Context closes Aeron instance if this fails to connect.
        aeronArchive = AeronArchive.connect(archiveContext.aeron(aeron).ownsAeronClient(true));

        final String logFileDir = configuration.logFileDir();
        final boolean enableIndexScan = configuration.enableIndexScan();
        if (logFileDir != null && enableIndexScan)
        {
            this.logFileDir = logFileDir;
        }
        else
        {
            this.logFileDir = null;
        }
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
        final FixPMessageConsumer fixPHandler,
        final boolean follow,
        final int archiveScannerStreamId)
    {
        final IntHashSet queryStreamIds = new IntHashSet();
        queryStreamIds.add(queryStreamId);
        scan(aeronChannel, queryStreamIds, fixHandler, fixPHandler, follow, archiveScannerStreamId);
    }

    public void scan(
        final String aeronChannel,
        final IntHashSet queryStreamIds,
        final FixMessageConsumer fixHandler,
        final FixPMessageConsumer fixPHandler,
        final boolean follow,
        final int archiveScannerStreamId)
    {
        if (fixHandler != null)
        {
            fixHandler.reset();
        }

        final Long2ObjectHashMap<PositionRange> recordingIdToPositionRange =
            scanIndexIfPossible(fixHandler, follow, queryStreamIds);

        try (Subscription replaySubscription = aeron.addSubscription(IPC_CHANNEL, archiveScannerStreamId))
        {
            final RecordingPoller[] pollers = makeRecordingPollers(
                aeronChannel, queryStreamIds, follow, recordingIdToPositionRange, replaySubscription);

            if (DEBUG_LOG_ARCHIVE_SCAN)
            {
                DebugLogger.log(ARCHIVE_SCAN, "Pollers: %s", pollers);
            }

            final StreamTimestampZipper timestampZipper = new StreamTimestampZipper(
                fixHandler, fixPHandler, compactionSize, !follow, pollers);

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

    private RecordingPoller[] makeRecordingPollers(
        final String aeronChannel,
        final IntHashSet queryStreamIds,
        final boolean follow,
        final Long2ObjectHashMap<PositionRange> recordingIdToPositionRange,
        final Subscription replaySubscription)
    {
        return queryStreamIds
            .stream()
            .flatMap(id ->
                lookupArchiveLocations(
                    id, follow, aeronChannel, recordingIdToPositionRange)
                    .stream()
                    // filter out empty streams as
                    // 1) they are empty - we don't need to poll them
                    // 2) we use empty length later within polling as a test for polling being finished
                    .filter(archiveLocation -> archiveLocation.length() != 0L)
                    .map(archiveLocation -> new RecordingPoller(replaySubscription, id, archiveLocation)))
            .toArray(RecordingPoller[]::new);
    }

    private Long2ObjectHashMap<PositionRange> scanIndexIfPossible(
        final FixMessageConsumer fixHandler, final boolean follow, final IntHashSet queryStreamIds)
    {
        if (DEBUG_LOG_ARCHIVE_SCAN)
        {
            DebugLogger.log(ARCHIVE_SCAN,
                "checking index,follow=" +
                follow +
                ",logFileDir=" + logFileDir +
                ",queryStreamIds=" + queryStreamIds);
        }

        // Don't support scan + continuous update query for now
        if (follow)
        {
            return null;
        }

        // need to know index location to do a scan
        if (logFileDir == null)
        {
            return null;
        }

        try
        {
            final IndexQuery indexQuery = ArchiveScanPlanner.extractIndexQuery(fixHandler);
            if (DEBUG_LOG_ARCHIVE_SCAN)
            {
                DebugLogger.log(ARCHIVE_SCAN, "indexQuery = " + indexQuery);
            }

            if (indexQuery == null)
            {
                return null;
            }

            final Long2ObjectHashMap<PositionRange> recordingIdToPositionRange = new Long2ObjectHashMap<>();
            for (final int streamId : queryStreamIds)
            {
                TimeIndexReader reader = streamIdToInboundTimeIndex.get(streamId);
                if (reader == null)
                {
                    reader = new TimeIndexReader(logFileDir, streamId);
                    streamIdToInboundTimeIndex.put(streamId, reader);
                }

                if (!reader.findPositionRange(indexQuery, recordingIdToPositionRange))
                {
                    return null;
                }
            }

            if (DEBUG_LOG_ARCHIVE_SCAN)
            {
                DebugLogger.log(ARCHIVE_SCAN, "recordingIdToPositionRange = " + recordingIdToPositionRange);
            }

            return recordingIdToPositionRange;
        }
        catch (final IllegalArgumentException e)
        {
            // Unable to create query plan
            return null;
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

    private List<ArchiveLocation> lookupArchiveLocations(
        final int queryStreamId,
        final boolean follow,
        final String aeronChannel,
        final Long2ObjectHashMap<PositionRange> recordingIdToPositionRange)
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

        // try to narrow down the scan range using the index
        if (recordingIdToPositionRange != null)
        {
            final Iterator<ArchiveLocation> iterator = archiveLocations.iterator();
            while (iterator.hasNext())
            {
                final ArchiveLocation location = iterator.next();

                final PositionRange positionRange = recordingIdToPositionRange.get(location.recordingId);
                if (positionRange == null)
                {
                    iterator.remove();
                }
                else
                {
                    final long startPosition = positionRange.startPosition();
                    final long endPosition = positionRange.endPosition();

                    if (location.stopPosition > endPosition)
                    {
                        location.stopPosition = endPosition;
                    }

                    if (location.startPosition < startPosition)
                    {
                        location.startPosition = startPosition;
                    }
                }
            }
        }

        return archiveLocations;
    }

    static class ArchiveLocation
    {
        final long recordingId;

        long startPosition;
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
            return "ArchiveLocation{" +
                "recordingId=" + recordingId +
                ", startPosition=" + startPosition +
                ", stopPosition=" + stopPosition +
                '}';
        }
    }

    class RecordingPoller implements StreamTimestampZipper.Poller
    {
        private final Subscription replaySubscription;
        private final int originalStreamId;

        private ArchiveLocation archiveLocation;

        long stopPosition;
        Image image;

        RecordingPoller(
            final Subscription replaySubscription,
            final int originalStreamId,
            final ArchiveLocation archiveLocation)
        {
            this.replaySubscription = replaySubscription;
            this.originalStreamId = originalStreamId;
            this.archiveLocation = archiveLocation;
        }

        public boolean isComplete()
        {
            return stopPosition != NULL_POSITION && image == null && archiveLocation == null;
        }

        public int poll(final FragmentAssembler fragmentAssembler)
        {
            if (image == null)
            {
                if (archiveLocation == null)
                {
                    return 0;
                }

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
                    archiveLocation = null;
                }

                return 1;
            }
            else
            {
                if (stopPosition != NULL_POSITION && image.position() >= stopPosition)
                {
                    image = null;
                    return 1;
                }
                else
                {
                    return image.poll(fragmentAssembler, fragmentLimit);
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

        public String toString()
        {
            return "RecordingPoller{" +
                "archiveLocations=" + archiveLocation +
                ", replaySubscription=" + replaySubscription +
                ", originalStreamId=" + originalStreamId +
                ", stopPosition=" + stopPosition +
                ", image=" + image +
                '}';
        }
    }

    public void close()
    {
        aeronArchive.close();
    }
}
