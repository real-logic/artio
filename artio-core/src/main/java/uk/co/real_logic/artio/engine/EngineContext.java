/*
 * Copyright 2015-2017 Real Logic Ltd.
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
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.UnavailableImageHandler;
import io.aeron.logbuffer.ExclusiveBufferClaim;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemEpochClock;
import uk.co.real_logic.artio.FixCounters;
import uk.co.real_logic.artio.StreamInformation;
import uk.co.real_logic.artio.engine.logger.*;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.protocol.Streams;
import uk.co.real_logic.artio.replication.ClusterSubscription;
import uk.co.real_logic.artio.replication.ClusterableStreams;
import uk.co.real_logic.artio.replication.StreamIdentifier;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static uk.co.real_logic.artio.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.dictionary.generation.Exceptions.suppressingClose;
import static uk.co.real_logic.artio.replication.ReservedValue.NO_FILTER;

public abstract class EngineContext implements AutoCloseable
{
    protected final NanoClock nanoClock;
    protected final EngineConfiguration configuration;
    protected final ErrorHandler errorHandler;
    protected final FixCounters fixCounters;
    protected final Aeron aeron;

    private final SequenceNumberIndexWriter sentSequenceNumberIndex;
    private final SequenceNumberIndexWriter receivedSequenceNumberIndex;
    private final CompletionPosition inboundCompletionPosition = new CompletionPosition();
    private final CompletionPosition outboundLibraryCompletionPosition = new CompletionPosition();
    private final CompletionPosition outboundClusterCompletionPosition = new CompletionPosition();

    protected Streams inboundLibraryStreams;
    protected Streams outboundLibraryStreams;
    // Indexers are owned by the archivingAgent
    protected Indexer inboundIndexer;
    protected Indexer outboundIndexer;
    protected Agent archivingAgent;

    public static EngineContext of(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final ExclusivePublication replayPublication,
        final FixCounters fixCounters,
        final Aeron aeron,
        final EngineDescriptorStore engineDescriptorStore)
    {
        if (configuration.isClustered())
        {
            if (!configuration.logInboundMessages() || !configuration.logOutboundMessages())
            {
                throw new IllegalArgumentException(
                    "If you are enabling clustering, then you must enable both inbound and outbound logging");
            }

            return new ClusterContext(
                configuration,
                errorHandler,
                replayPublication,
                fixCounters,
                aeron,
                engineDescriptorStore);
        }
        else
        {
            return new SoloContext(
                configuration,
                errorHandler,
                replayPublication,
                fixCounters,
                aeron);
        }
    }

    public EngineContext(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final FixCounters fixCounters,
        final Aeron aeron)
    {
        this.configuration = configuration;
        this.errorHandler = errorHandler;
        this.fixCounters = fixCounters;
        this.aeron = aeron;
        this.nanoClock = configuration.nanoClock();

        try
        {
            sentSequenceNumberIndex = new SequenceNumberIndexWriter(
                configuration.sentSequenceNumberBuffer(),
                configuration.sentSequenceNumberIndex(),
                errorHandler,
                OUTBOUND_LIBRARY_STREAM);
            receivedSequenceNumberIndex = new SequenceNumberIndexWriter(
                configuration.receivedSequenceNumberBuffer(),
                configuration.receivedSequenceNumberIndex(),
                errorHandler,
                INBOUND_LIBRARY_STREAM);
        }
        catch (final Exception e)
        {
            suppressingClose(this, e);

            throw e;
        }
    }

    protected void newStreams(final ClusterableStreams node)
    {
        inboundLibraryStreams = new Streams(
            node, fixCounters.failedInboundPublications(), INBOUND_LIBRARY_STREAM, nanoClock,
            configuration.inboundMaxClaimAttempts());
        outboundLibraryStreams = new Streams(
            node, fixCounters.failedOutboundPublications(), OUTBOUND_LIBRARY_STREAM, nanoClock,
            configuration.outboundMaxClaimAttempts());
    }

    protected ReplayIndex newReplayIndex(
        final int cacheSetSize,
        final int cacheNumSets,
        final String logFileDir,
        final int streamId)
    {
        return new ReplayIndex(
            logFileDir,
            streamId,
            configuration.replayIndexFileSize(),
            cacheNumSets,
            cacheSetSize,
            LoggerUtil::map,
            ReplayIndexDescriptor.replayPositionBuffer(logFileDir, streamId),
            errorHandler);
    }

    protected ReplayQuery newReplayQuery(final ArchiveReader archiveReader, final IdleStrategy idleStrategy)
    {
        final String logFileDir = configuration.logFileDir();
        final int cacheSetSize = configuration.loggerCacheSetSize();
        final int cacheNumSets = configuration.loggerCacheNumSets();
        final int streamId = archiveReader.fullStreamId().streamId();
        return new ReplayQuery(
            logFileDir,
            cacheNumSets,
            cacheSetSize,
            LoggerUtil::mapExistingFile,
            archiveReader,
            streamId,
            idleStrategy);
    }

    public void close()
    {
        sentSequenceNumberIndex.close();
        receivedSequenceNumberIndex.close();
    }

    protected ArchiveReader archiveReader(final StreamIdentifier streamId)
    {
        return archiveReader(streamId, NO_FILTER);
    }

    protected ArchiveReader archiveReader(final StreamIdentifier streamId, final int reservedValueFilter)
    {
        return new ArchiveReader(
            LoggerUtil.newArchiveMetaData(configuration.logFileDir()),
            configuration.loggerCacheNumSets(),
            configuration.loggerCacheSetSize(),
            streamId,
            reservedValueFilter);
    }

    protected Archiver archiver(final StreamIdentifier streamId, final CompletionPosition completionPosition)
    {
        return new Archiver(
            LoggerUtil.newArchiveMetaData(configuration.logFileDir()),
            configuration.loggerCacheNumSets(),
            configuration.loggerCacheSetSize(),
            streamId,
            configuration.agentNamePrefix(),
            completionPosition);
    }

    protected Replayer newReplayer(
        final ExclusivePublication replayPublication, final ArchiveReader outboundArchiveReader)
    {
        return new Replayer(
            newReplayQuery(outboundArchiveReader, configuration.archiverIdleStrategy()),
            replayPublication,
            new ExclusiveBufferClaim(),
            configuration.archiverIdleStrategy(),
            errorHandler,
            configuration.outboundMaxClaimAttempts(),
            inboundLibraryStreams.subscription("replayer"),
            configuration.agentNamePrefix(),
            new SystemEpochClock(),
            configuration.gapfillOnReplayMessageTypes(),
            configuration.replayHandler());
    }

    protected void newIndexers(
        final ArchiveReader inboundArchiveReader,
        final ArchiveReader outboundArchiveReader,
        final Index extraOutboundIndex)
    {
        final int cacheSetSize = configuration.loggerCacheSetSize();
        final int cacheNumSets = configuration.loggerCacheNumSets();
        final String logFileDir = configuration.logFileDir();

        final ReplayIndex replayIndex = newReplayIndex(cacheSetSize, cacheNumSets, logFileDir, INBOUND_LIBRARY_STREAM);

        inboundIndexer = new Indexer(
            asList(replayIndex, receivedSequenceNumberIndex),
            inboundArchiveReader,
            inboundLibraryStreams.subscription("inboundIndexer"),
            configuration.agentNamePrefix(),
            inboundCompletionPosition);

        final List<Index> outboundIndices = new ArrayList<>();
        outboundIndices.add(newReplayIndex(cacheSetSize, cacheNumSets, logFileDir, OUTBOUND_LIBRARY_STREAM));
        outboundIndices.add(sentSequenceNumberIndex);
        if (extraOutboundIndex != null)
        {
            outboundIndices.add(extraOutboundIndex);
        }

        outboundIndexer = new Indexer(
            outboundIndices,
            outboundArchiveReader,
            outboundLibraryStreams.subscription("outboundIndexer"),
            configuration.agentNamePrefix(),
            outboundLibraryCompletionPosition);
    }

    public abstract Streams outboundLibraryStreams();

    public abstract Streams inboundLibraryStreams();

    public abstract ClusterSubscription outboundClusterSubscription();

    // Each invocation should return a new instance of the subscription
    public Subscription outboundLibrarySubscription(
        final String name, final UnavailableImageHandler unavailableImageHandler)
    {
        final Subscription subscription = aeron.addSubscription(
            configuration.libraryAeronChannel(), OUTBOUND_LIBRARY_STREAM, null, unavailableImageHandler);
        StreamInformation.print(name, subscription, configuration);
        return subscription;
    }

    public abstract ReplayQuery inboundReplayQuery();

    public abstract ClusterableStreams streams();

    public abstract GatewayPublication inboundLibraryPublication();

    public CompletionPosition inboundCompletionPosition()
    {
        return inboundCompletionPosition;
    }

    public CompletionPosition outboundLibraryCompletionPosition()
    {
        return outboundLibraryCompletionPosition;
    }

    public CompletionPosition outboundClusterCompletionPosition()
    {
        return outboundClusterCompletionPosition;
    }

    void completeDuringStartup()
    {
        inboundCompletionPosition.completeDuringStartup();
        outboundLibraryCompletionPosition.completeDuringStartup();
        outboundClusterCompletionPosition.completeDuringStartup();
    }

    Agent archivingAgent()
    {
        return archivingAgent;
    }
}
