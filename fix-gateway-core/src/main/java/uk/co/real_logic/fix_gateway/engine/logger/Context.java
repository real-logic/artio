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
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.replication.ClusterableNode;

/**
 * Top level entry point for the whole logging module.
 */
public abstract class Context implements AutoCloseable
{
    public static Context of(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final Publication replayPublication,
        final FixCounters fixCounters,
        final Aeron aeron)
    {
        final SequenceNumberIndexWriter sentSequenceNumberIndexWriter = new SequenceNumberIndexWriter(
            configuration.sentSequenceNumberBuffer(), configuration.sentSequenceNumberIndex(), errorHandler);
        final SequenceNumberIndexWriter receivedSequenceNumberIndex = new SequenceNumberIndexWriter(
            configuration.receivedSequenceNumberBuffer(), configuration.receivedSequenceNumberIndex(), errorHandler);

        if (configuration.isClustered())
        {
            if (!configuration.logInboundMessages() || !configuration.logOutboundMessages())
            {
                throw new IllegalArgumentException(
                    "If you are enabling clustering, then you must enable both inbound and outbound logging");
            }

            return null;
        }
        else
        {
            return new SoloContext(
                configuration,
                errorHandler,
                replayPublication,
                sentSequenceNumberIndexWriter,
                receivedSequenceNumberIndex,
                fixCounters,
                aeron).init();
        }
    }

    public abstract ReplayQuery inboundReplayQuery();

    public abstract ClusterableNode node();

    public abstract void start();

    public abstract void close();

    public abstract Streams outboundLibraryStreams();

    public abstract Streams inboundLibraryStreams();
}
