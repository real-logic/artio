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
package uk.co.real_logic.fix_gateway.engine;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.CompositeAgent;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.StreamInformation;
import uk.co.real_logic.fix_gateway.dictionary.generation.Exceptions;
import uk.co.real_logic.fix_gateway.engine.logger.*;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.replication.ClusterSubscription;
import uk.co.real_logic.fix_gateway.replication.ClusterablePublication;
import uk.co.real_logic.fix_gateway.replication.ClusterableStreams;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import java.util.ArrayList;
import java.util.List;

import static uk.co.real_logic.fix_gateway.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.dictionary.generation.Exceptions.suppressingClose;

class SoloContext extends EngineContext
{
    private final ExclusivePublication replayPublication;
    private final List<Archiver> archivers = new ArrayList<>();
    private final StreamIdentifier inboundStreamId;
    private final StreamIdentifier outboundStreamId;
    private final ClusterableStreams node;

    private ArchiveReader outboundArchiveReader;
    private ArchiveReader inboundArchiveReader;
    private Archiver inboundArchiver;
    private Archiver outboundArchiver;

    SoloContext(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final ExclusivePublication replayPublication,
        final FixCounters fixCounters,
        final Aeron aeron)
    {
        super(configuration, errorHandler, fixCounters, aeron);
        try
        {
            this.replayPublication = replayPublication;

            final String channel = configuration.libraryAeronChannel();
            this.inboundStreamId = new StreamIdentifier(channel, INBOUND_LIBRARY_STREAM);
            this.outboundStreamId = new StreamIdentifier(channel, OUTBOUND_LIBRARY_STREAM);

            node = newNode();
            newStreams(node);
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

    private ClusterableStreams newNode()
    {
        return ClusterableStreams.solo(
            aeron, configuration.libraryAeronChannel(), configuration.printAeronStreamIdentifiers());
    }

    private void newArchivingAgent()
    {
        if (configuration.logOutboundMessages())
        {
            newIndexers(
                inboundArchiveReader,
                outboundArchiveReader,
                new SoloPositionSender(inboundLibraryPublication()));

            final Replayer replayer = newReplayer(replayPublication, outboundArchiveReader);

            archiverSubscription(inboundArchiver, inboundStreamId);
            archiverSubscription(outboundArchiver, outboundStreamId);

            final List<Agent> agents = new ArrayList<>(archivers);
            agents.add(inboundIndexer);
            agents.add(outboundIndexer);
            agents.add(replayer);

            archivingAgent = new CompositeAgent(agents);
        }
        else
        {
            final GatewayPublication replayGatewayPublication =
                new GatewayPublication(
                    ClusterablePublication.solo(replayPublication),
                    fixCounters.failedReplayPublications(),
                    configuration.archiverIdleStrategy(),
                    nanoClock,
                    configuration.outboundMaxClaimAttempts()
                );
            archivingAgent = new GapFiller(
                inboundLibraryStreams.subscription("replayer"),
                replayGatewayPublication,
                configuration.agentNamePrefix());
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

    public ClusterSubscription outboundClusterSubscription()
    {
        return null;
    }

    public ClusterableStreams streams()
    {
        return node;
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

    public void close()
    {
        Exceptions.closeAll(super::close, inboundArchiveReader, outboundArchiveReader);
    }
}
