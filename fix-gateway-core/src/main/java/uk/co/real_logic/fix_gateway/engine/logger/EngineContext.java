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
package uk.co.real_logic.fix_gateway.engine.logger;

import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.replication.ClusterableNode;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import static uk.co.real_logic.fix_gateway.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;

public abstract class EngineContext implements AutoCloseable
{
    protected final EngineConfiguration configuration;
    protected final ErrorHandler errorHandler;
    protected final FixCounters fixCounters;
    protected final Aeron aeron;
    protected final SequenceNumberIndexWriter sentSequenceNumberIndex;
    protected final SequenceNumberIndexWriter receivedSequenceNumberIndex;

    protected Streams inboundLibraryStreams;
    protected Streams outboundLibraryStreams;
    protected AgentRunner loggingRunner;

    public static EngineContext of(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final Publication replayPublication,
        final FixCounters fixCounters,
        final Aeron aeron)
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
                aeron);
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

    protected void initStreams(final ClusterableNode node)
    {
        final NanoClock nanoClock = new SystemNanoClock();
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
            configuration.indexFileSize(),
            cacheNumSets,
            cacheSetSize,
            LoggerUtil::map,
            ReplayIndex.replayBuffer(logFileDir, streamId),
            errorHandler);
    }

    protected ReplayQuery newReplayQuery(final ArchiveReader archiveReader)
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
            streamId);
    }

    public void close()
    {
        sentSequenceNumberIndex.close();
        receivedSequenceNumberIndex.close();
    }

    protected ArchiveReader archiveReader(final StreamIdentifier streamId)
    {
        return new ArchiveReader(
            LoggerUtil.newArchiveMetaData(configuration.logFileDir()),
            configuration.loggerCacheNumSets(),
            configuration.loggerCacheSetSize(),
            streamId);
    }

    protected AgentRunner newRunner(final Agent loggingAgent)
    {
        return new AgentRunner(configuration.loggerIdleStrategy(), errorHandler, null, loggingAgent);
    }

    protected Archiver archiver(final StreamIdentifier streamId)
    {
        final int cacheNumSets = configuration.loggerCacheNumSets();
        final int cacheSetSize = configuration.loggerCacheSetSize();
        return new Archiver(
            LoggerUtil.newArchiveMetaData(configuration.logFileDir()),
            cacheNumSets,
            cacheSetSize,
            streamId);
    }

    public abstract Streams outboundLibraryStreams();

    public abstract Streams inboundLibraryStreams();

    public abstract ReplayQuery inboundReplayQuery();

    public abstract ClusterableNode node();

    public abstract void start();
}
