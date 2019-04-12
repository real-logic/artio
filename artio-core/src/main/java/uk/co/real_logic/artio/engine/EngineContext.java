/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.BufferClaim;
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
import uk.co.real_logic.artio.protocol.Streams;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static uk.co.real_logic.artio.dictionary.generation.Exceptions.suppressingClose;

public class EngineContext implements AutoCloseable
{
    private final Clock clock;
    private final EngineConfiguration configuration;
    private final ErrorHandler errorHandler;
    private final FixCounters fixCounters;
    private final Aeron aeron;
    private final SenderSequenceNumbers senderSequenceNumbers;
    private final AeronArchive aeronArchive;
    private final RecordingCoordinator recordingCoordinator;
    private final ExclusivePublication replayPublication;
    private final SequenceNumberIndexWriter sentSequenceNumberIndex;
    private final SequenceNumberIndexWriter receivedSequenceNumberIndex;
    private final CompletionPosition inboundCompletionPosition = new CompletionPosition();
    private final CompletionPosition outboundLibraryCompletionPosition = new CompletionPosition();
    private final CompletionPosition outboundClusterCompletionPosition = new CompletionPosition();

    private Streams inboundLibraryStreams;
    private Streams outboundLibraryStreams;

    // Indexers are owned by the archivingAgent
    private Indexer inboundIndexer;
    private Indexer outboundIndexer;
    private Agent archivingAgent;

    EngineContext(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final ExclusivePublication replayPublication,
        final FixCounters fixCounters,
        final Aeron aeron,
        final AeronArchive aeronArchive,
        final RecordingCoordinator recordingCoordinator)
    {
        this.configuration = configuration;
        this.errorHandler = errorHandler;
        this.fixCounters = fixCounters;
        this.aeron = aeron;
        this.clock = configuration.clock();
        this.replayPublication = replayPublication;
        this.aeronArchive = aeronArchive;
        this.recordingCoordinator = recordingCoordinator;

        senderSequenceNumbers = new SenderSequenceNumbers(configuration.framerIdleStrategy());

        try
        {
            sentSequenceNumberIndex = new SequenceNumberIndexWriter(
                configuration.sentSequenceNumberBuffer(),
                configuration.sentSequenceNumberIndex(),
                errorHandler,
                configuration.outboundLibraryStream(),
                recordingCoordinator.outboundRecordingIdLookup());
            receivedSequenceNumberIndex = new SequenceNumberIndexWriter(
                configuration.receivedSequenceNumberBuffer(),
                configuration.receivedSequenceNumberIndex(),
                errorHandler,
                configuration.inboundLibraryStream(),
                recordingCoordinator.inboundRecordingIdLookup());

            newStreams();
            newArchivingAgent();
        }
        catch (final Exception e)
        {
            completeDuringStartup();

            suppressingClose(this, e);

            throw e;
        }
    }

    private void newStreams()
    {
        final String libraryAeronChannel = configuration.libraryAeronChannel();
        final boolean printAeronStreamIdentifiers = configuration.printAeronStreamIdentifiers();

        inboundLibraryStreams = new Streams(
            aeron,
            libraryAeronChannel,
            printAeronStreamIdentifiers,
            fixCounters.failedInboundPublications(),
            configuration.inboundLibraryStream(),
            clock,
            configuration.inboundMaxClaimAttempts(),
            recordingCoordinator);
        outboundLibraryStreams = new Streams(
            aeron,
            libraryAeronChannel,
            printAeronStreamIdentifiers,
            fixCounters.failedOutboundPublications(),
            configuration.outboundLibraryStream(),
            clock,
            configuration.outboundMaxClaimAttempts(),
            recordingCoordinator);
    }

    private ReplayIndex newReplayIndex(
        final int cacheSetSize,
        final int cacheNumSets,
        final String logFileDir,
        final int streamId,
        final RecordingIdLookup recordingIdLookup)
    {
        return new ReplayIndex(
            logFileDir,
            streamId,
            configuration.replayIndexFileSize(),
            cacheNumSets,
            cacheSetSize,
            LoggerUtil::map,
            ReplayIndexDescriptor.replayPositionBuffer(logFileDir, streamId),
            errorHandler,
            recordingIdLookup);
    }

    private ReplayQuery newReplayQuery(final IdleStrategy idleStrategy, final int streamId)
    {
        final String logFileDir = configuration.logFileDir();
        final int cacheSetSize = configuration.loggerCacheSetSize();
        final int cacheNumSets = configuration.loggerCacheNumSets();
        final int archiveReplayStream = configuration.archiveReplayStream();

        return new ReplayQuery(
            logFileDir,
            cacheNumSets,
            cacheSetSize,
            LoggerUtil::mapExistingFile,
            streamId,
            idleStrategy,
            aeronArchive,
            errorHandler,
            archiveReplayStream);
    }

    private Replayer newReplayer(
        final ExclusivePublication replayPublication)
    {
        return new Replayer(
            newReplayQuery(configuration.archiverIdleStrategy(), configuration.outboundLibraryStream()),
            replayPublication,
            new BufferClaim(),
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

    private void newIndexers()
    {
        final int cacheSetSize = configuration.loggerCacheSetSize();
        final int cacheNumSets = configuration.loggerCacheNumSets();
        final String logFileDir = configuration.logFileDir();

        final ReplayIndex inboundReplayIndex = newReplayIndex(
            cacheSetSize,
            cacheNumSets,
            logFileDir,
            configuration.inboundLibraryStream(),
            recordingCoordinator.inboundRecordingIdLookup());

        inboundIndexer = new Indexer(
            asList(inboundReplayIndex, receivedSequenceNumberIndex),
            inboundLibraryStreams.subscription("inboundIndexer"),
            configuration.agentNamePrefix(),
            inboundCompletionPosition,
            aeronArchive,
            errorHandler,
            configuration.archiveReplayStream());

        final List<Index> outboundIndices = new ArrayList<>();
        outboundIndices.add(newReplayIndex(
            cacheSetSize,
            cacheNumSets,
            logFileDir,
            configuration.outboundLibraryStream(),
            recordingCoordinator.outboundRecordingIdLookup()));
        outboundIndices.add(sentSequenceNumberIndex);
        outboundIndices.add(new PositionSender(inboundPublication()));

        outboundIndexer = new Indexer(
            outboundIndices,
            outboundLibraryStreams.subscription("outboundIndexer"),
            configuration.agentNamePrefix(),
            outboundLibraryCompletionPosition,
            aeronArchive,
            errorHandler,
            configuration.archiveReplayStream());
    }

    private void newArchivingAgent()
    {
        if (configuration.logOutboundMessages())
        {
            newIndexers();

            final Replayer replayer = newReplayer(replayPublication);

            final List<Agent> agents = new ArrayList<>();
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

    public Streams outboundLibraryStreams()
    {
        return outboundLibraryStreams;
    }

    // Each invocation should return a new instance of the subscription
    public Subscription outboundLibrarySubscription(
        final String name, final UnavailableImageHandler unavailableImageHandler)
    {
        final Subscription subscription = aeron.addSubscription(
            configuration.libraryAeronChannel(),
            configuration.outboundLibraryStream(),
            null,
            unavailableImageHandler);
        StreamInformation.print(name, subscription, configuration);
        return subscription;
    }

    public ReplayQuery inboundReplayQuery()
    {
        if (!configuration.logInboundMessages())
        {
            return null;
        }

        return newReplayQuery(configuration.framerIdleStrategy(), configuration.inboundLibraryStream());
    }

    public GatewayPublication inboundPublication()
    {
        return inboundLibraryStreams.gatewayPublication(
            configuration.framerIdleStrategy(), "inboundPublication");
    }

    public CompletionPosition inboundCompletionPosition()
    {
        return inboundCompletionPosition;
    }

    public CompletionPosition outboundLibraryCompletionPosition()
    {
        return outboundLibraryCompletionPosition;
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
            sentSequenceNumberIndex, receivedSequenceNumberIndex);
    }
}
