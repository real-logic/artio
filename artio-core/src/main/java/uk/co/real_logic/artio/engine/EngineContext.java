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
import org.agrona.concurrent.CompositeAgent;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import uk.co.real_logic.artio.Clock;
import uk.co.real_logic.artio.FixCounters;
import uk.co.real_logic.artio.StreamInformation;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.engine.logger.*;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.protocol.StreamIdentifier;
import uk.co.real_logic.artio.protocol.Streams;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static uk.co.real_logic.artio.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.dictionary.generation.Exceptions.suppressingClose;
import static uk.co.real_logic.artio.protocol.ReservedValue.NO_FILTER;

public class EngineContext implements AutoCloseable
{
    private final Clock clock;
    private final EngineConfiguration configuration;
    private final ErrorHandler errorHandler;
    private final FixCounters fixCounters;
    private final Aeron aeron;
    private final SenderSequenceNumbers senderSequenceNumbers;
    private final StartRecordingCoordinator startRecordingCoordinator;
    private final ExclusivePublication replayPublication;
    private final StreamIdentifier inboundStreamId;
    private final SequenceNumberIndexWriter sentSequenceNumberIndex;
    private final SequenceNumberIndexWriter receivedSequenceNumberIndex;
    private final CompletionPosition inboundCompletionPosition = new CompletionPosition();
    private final CompletionPosition outboundLibraryCompletionPosition = new CompletionPosition();
    private final CompletionPosition outboundClusterCompletionPosition = new CompletionPosition();
    private final List<Archiver> archivers = new ArrayList<>();
    private final StreamIdentifier outboundStreamId;

    private ArchiveReader outboundArchiveReader;
    private ArchiveReader inboundArchiveReader;
    private Archiver inboundArchiver;
    private Archiver outboundArchiver;
    private Streams inboundLibraryStreams;
    private Streams outboundLibraryStreams;

    // Indexers are owned by the archivingAgent
    private Indexer inboundIndexer;
    private Indexer outboundIndexer;
    private Agent archivingAgent;

    public EngineContext(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final ExclusivePublication replayPublication,
        final FixCounters fixCounters,
        final Aeron aeron,
        final StartRecordingCoordinator startRecordingCoordinator)
    {
        this.configuration = configuration;
        this.errorHandler = errorHandler;
        this.fixCounters = fixCounters;
        this.aeron = aeron;
        this.clock = configuration.clock();
        this.replayPublication = replayPublication;
        this.startRecordingCoordinator = startRecordingCoordinator;

        senderSequenceNumbers = new SenderSequenceNumbers(configuration.framerIdleStrategy());

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

            final String channel = configuration.libraryAeronChannel();
            this.inboundStreamId = new StreamIdentifier(channel, INBOUND_LIBRARY_STREAM);
            this.outboundStreamId = new StreamIdentifier(channel, OUTBOUND_LIBRARY_STREAM);

            newStreams();
            newArchival();
            newArchivingAgent();
        }
        catch (final Exception e)
        {
            completeDuringStartup();

            suppressingClose(this, e);

            throw e;
        }
    }

    protected void newStreams()
    {
        final String libraryAeronChannel = configuration.libraryAeronChannel();
        final boolean printAeronStreamIdentifiers = configuration.printAeronStreamIdentifiers();

        inboundLibraryStreams = new Streams(
            aeron,
            libraryAeronChannel,
            printAeronStreamIdentifiers,
            fixCounters.failedInboundPublications(),
            INBOUND_LIBRARY_STREAM,
            clock,
            configuration.inboundMaxClaimAttempts(),
            startRecordingCoordinator);
        outboundLibraryStreams = new Streams(
            aeron,
            libraryAeronChannel,
            printAeronStreamIdentifiers,
            fixCounters.failedOutboundPublications(),
            OUTBOUND_LIBRARY_STREAM, clock,
            configuration.outboundMaxClaimAttempts(),
            startRecordingCoordinator);
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
            configuration.replayHandler(),
            senderSequenceNumbers);
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

    protected void newArchivingAgent()
    {
        if (configuration.logOutboundMessages())
        {
            newIndexers(
                inboundArchiveReader,
                outboundArchiveReader,
                new PositionSender(inboundLibraryPublication()));

            final Replayer replayer = newReplayer(replayPublication, outboundArchiveReader);

            if (configuration.logInboundMessages())
            {
                archiverSubscription(inboundArchiver, inboundStreamId);
            }


            if (configuration.logOutboundMessages())
            {
                archiverSubscription(outboundArchiver, outboundStreamId);
            }

            final List<Agent> agents = new ArrayList<>(archivers);
            agents.add(inboundIndexer);
            agents.add(outboundIndexer);
            agents.add(replayer);

            archivingAgent = new CompositeAgent(agents);
        }
        else
        {
            final GatewayPublication replayGatewayPublication = new GatewayPublication(
                replayPublication,
                fixCounters.failedReplayPublications(),
                configuration.archiverIdleStrategy(),
                clock,
                configuration.outboundMaxClaimAttempts());

            archivingAgent = new GapFiller(
                inboundLibraryStreams.subscription("replayer"),
                replayGatewayPublication,
                configuration.agentNamePrefix(),
                senderSequenceNumbers);
        }
    }

    private void archiverSubscription(final Archiver archiver, final StreamIdentifier streamId)
    {

        final Subscription subscription = aeron.addSubscription(streamId.channel(), streamId.streamId());
        StreamInformation.print("Archiver", subscription, configuration);
        archiver.subscription(subscription);
    }

    public void newArchival()
    {
        if (configuration.logInboundMessages())
        {
            inboundArchiver = addArchiver(inboundStreamId, inboundCompletionPosition());
            inboundArchiveReader = archiveReader(inboundStreamId);
        }

        if (configuration.logOutboundMessages())
        {
            outboundArchiver = addArchiver(outboundStreamId, outboundLibraryCompletionPosition());
            outboundArchiveReader = archiveReader(outboundStreamId);
        }
    }

    private Archiver addArchiver(final StreamIdentifier streamId, final CompletionPosition completionPosition)
    {
        final Archiver archiver = archiver(streamId, completionPosition);
        archivers.add(archiver);
        return archiver;
    }

    public Streams outboundLibraryStreams()
    {
        return outboundLibraryStreams;
    }

    public Streams inboundLibraryStreams()
    {
        return inboundLibraryStreams;
    }

    // Each invocation should return a new instance of the subscription
    public Subscription outboundLibrarySubscription(
        final String name, final UnavailableImageHandler unavailableImageHandler)
    {
        final Subscription subscription = aeron.addSubscription(
            configuration.libraryAeronChannel(), OUTBOUND_LIBRARY_STREAM, null, unavailableImageHandler);
        StreamInformation.print(name, subscription, configuration);
        return subscription;
    }

    public ReplayQuery inboundReplayQuery()
    {
        if (!configuration.logInboundMessages())
        {
            return null;
        }

        final ArchiveReader archiveReader = archiveReader(inboundStreamId);
        return newReplayQuery(archiveReader, configuration.framerIdleStrategy());
    }

    public GatewayPublication inboundLibraryPublication()
    {
        return inboundLibraryStreams.gatewayPublication(
            configuration.framerIdleStrategy(), "inboundLibraryPublication");
    }

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

    public SenderSequenceNumbers senderSequenceNumbers()
    {
        return senderSequenceNumbers;
    }

    public void close()
    {
        Exceptions.closeAll(
            sentSequenceNumberIndex, receivedSequenceNumberIndex, inboundArchiveReader, outboundArchiveReader);
    }
}
