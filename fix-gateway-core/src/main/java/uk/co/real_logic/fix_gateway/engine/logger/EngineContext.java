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
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.replication.ClusterableNode;

import static uk.co.real_logic.fix_gateway.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;

public abstract class EngineContext implements AutoCloseable
{
    protected final EngineConfiguration configuration;
    protected final FixCounters fixCounters;
    protected final Aeron aeron;
    protected final SequenceNumberIndexWriter sentSequenceNumberIndex;
    protected final SequenceNumberIndexWriter receivedSequenceNumberIndex;

    protected Streams inboundLibraryStreams;
    protected Streams outboundLibraryStreams;

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
        final ErrorHandler errorHandler, final FixCounters fixCounters,
        final Aeron aeron)
    {
        this.configuration = configuration;
        this.fixCounters = fixCounters;
        this.aeron = aeron;

        sentSequenceNumberIndex = new SequenceNumberIndexWriter(
            configuration.sentSequenceNumberBuffer(), configuration.sentSequenceNumberIndex(), errorHandler);
        receivedSequenceNumberIndex = new SequenceNumberIndexWriter(
            configuration.receivedSequenceNumberBuffer(), configuration.receivedSequenceNumberIndex(), errorHandler);
    }

    public abstract ReplayQuery inboundReplayQuery();

    public abstract ClusterableNode node();

    public abstract void start();

    public void close()
    {
        sentSequenceNumberIndex.close();
        receivedSequenceNumberIndex.close();
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

    public abstract Streams outboundLibraryStreams();

    public abstract Streams inboundLibraryStreams();
}
