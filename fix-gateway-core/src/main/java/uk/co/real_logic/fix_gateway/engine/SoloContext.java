/*
 * Copyright 2015-2016 Real Logic Ltd.
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
import io.aeron.Publication;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.CompositeAgent;
import org.agrona.concurrent.SystemNanoClock;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.engine.logger.*;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.replication.*;

import java.util.ArrayList;
import java.util.List;

import static org.agrona.concurrent.AgentRunner.startOnThread;
import static uk.co.real_logic.fix_gateway.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.replication.ReservedValue.NO_FILTER;

class SoloContext extends EngineContext
{
    private final Publication replayPublication;
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
        final Publication replayPublication,
        final FixCounters fixCounters,
        final Aeron aeron)
    {
        super(configuration, errorHandler, fixCounters, aeron);
        this.replayPublication = replayPublication;

        final String channel = configuration.libraryAeronChannel();
        this.inboundStreamId = new StreamIdentifier(channel, INBOUND_LIBRARY_STREAM);
        this.outboundStreamId = new StreamIdentifier(channel, OUTBOUND_LIBRARY_STREAM);

        node = initNode();
        newStreams(node);
        newArchival();
        newLoggingRunner();
    }

    private ClusterableStreams initNode()
    {
        return ClusterableStreams.solo(aeron, configuration.libraryAeronChannel());
    }

    private void newLoggingRunner()
    {
        if (configuration.logOutboundMessages())
        {
            newIndexers(
                inboundArchiveReader,
                outboundArchiveReader,
                new SoloPositionSender(inboundLibraryPublication()));

            final Replayer replayer = newReplayer(replayPublication, outboundArchiveReader);

            inboundArchiver.subscription(
                aeron.addSubscription(inboundStreamId.channel(), inboundStreamId.streamId()));
            outboundArchiver.subscription(
                aeron.addSubscription(outboundStreamId.channel(), outboundStreamId.streamId()));

            final List<Agent> agents = new ArrayList<>(archivers);
            agents.add(inboundIndexer);
            agents.add(outboundIndexer);
            agents.add(replayer);

            final Agent loggingAgent = new CompositeAgent(agents);

            loggingRunner = newRunner(loggingAgent);
        }
        else
        {
            final GatewayPublication replayGatewayPublication =
                new GatewayPublication(
                    ClusterablePublication.solo(replayPublication),
                    fixCounters.failedReplayPublications(),
                    configuration.loggerIdleStrategy(),
                    new SystemNanoClock(),
                    configuration.outboundMaxClaimAttempts()
                );
            final GapFiller gapFiller = new GapFiller(
                inboundLibraryStreams.subscription(),
                replayGatewayPublication);
            loggingRunner = newRunner(gapFiller);
        }
    }

    public void newArchival()
    {
        if (configuration.logInboundMessages())
        {
            inboundArchiver = addArchiver(inboundStreamId);
            inboundArchiveReader = archiveReader(inboundStreamId, NO_FILTER);
        }

        if (configuration.logOutboundMessages())
        {
            outboundArchiver = addArchiver(outboundStreamId);
            outboundArchiveReader = archiveReader(outboundStreamId, NO_FILTER);
        }
    }

    private Archiver addArchiver(final StreamIdentifier streamId)
    {
        final Archiver archiver = archiver(streamId);
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

    public SoloSubscription outboundLibrarySubscription()
    {
        return (SoloSubscription) outboundLibraryStreams().subscription();
    }

    public ClusterableSubscription outboundClusterSubscription()
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

        final ArchiveReader archiveReader =
            archiveReader(inboundStreamId, NO_FILTER);
        return newReplayQuery(archiveReader);
    }

    public void start()
    {
        if (loggingRunner == null)
        {
            loggingRunner = newRunner(new CompositeAgent(archivers));
        }

        startOnThread(loggingRunner);
    }

    public GatewayPublication inboundLibraryPublication()
    {
        return inboundLibraryStreams.gatewayPublication(configuration.framerIdleStrategy());
    }

    public void close()
    {
        if (loggingRunner != null)
        {
            loggingRunner.close();
        }
        else
        {
            archivers.forEach(Archiver::onClose);
        }

        super.close();

        CloseHelper.close(inboundArchiveReader);
        CloseHelper.close(outboundArchiveReader);
    }
}
