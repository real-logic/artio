/*
 * Copyright 2022 Monotonic Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import jdk.internal.org.objectweb.asm.Handle;
import org.agrona.CloseHelper;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.engine.EngineReproductionClock;
import uk.co.real_logic.artio.engine.ReproductionConfiguration;
import uk.co.real_logic.artio.engine.logger.FixArchiveScanner;
import uk.co.real_logic.artio.engine.logger.FixArchiveScanningAgent;
import uk.co.real_logic.artio.engine.logger.StreamTimestampZipper;

import static uk.co.real_logic.artio.engine.logger.FixMessageLogger.Configuration.DEFAULT_COMPACTION_SIZE;
import static uk.co.real_logic.artio.engine.logger.FixMessageLogger.Configuration.DEFAULT_MAXIMUM_BUFFER_SIZE;

class ReproductionPoller implements Continuation
{
    private final EngineReproductionClock clock;
    private final ReproductionConfiguration configuration;
    private final ReproductionProtocolHandler protocolHandler;
    private final String logFileDir;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final String aeronChannel;
    private final int inboundLibraryStreamId;
    private final IdleStrategy idleStrategy;

    // NB: we should never close this class as it closes the Archiver
    private FixArchiveScanningAgent archiveScanningAgent;

    ReproductionPoller(
        final ReproductionConfiguration configuration,
        final TcpChannelSupplier channelSupplier,
        final IdleStrategy idleStrategy,
        final String logFileDir,
        final Aeron aeron,
        final AeronArchive archive,
        final String aeronChannel,
        final int inboundLibraryStreamId)
    {
        this.configuration = configuration;
        this.idleStrategy = idleStrategy;

        clock = configuration.clock();
        protocolHandler = new ReproductionProtocolHandler(
            (ReproductionTcpChannelSupplier)channelSupplier,
            clock);
        this.logFileDir = logFileDir;
        this.aeron = aeron;
        this.archive = archive;
        this.aeronChannel = aeronChannel;
        this.inboundLibraryStreamId = inboundLibraryStreamId;
    }

    public long attempt()
    {
        final FixArchiveScanningAgent archiveScanningAgent = this.archiveScanningAgent;
        if (archiveScanningAgent != null)
        {
            System.out.println("polling underneath");
            if (archiveScanningAgent.poll(1))
            {
                // TODO: need a way of getting this event back to the user / API
                System.out.println("We're done!");
                this.archiveScanningAgent = null;
                return COMPLETE;
            }
        }

        return Publication.BACK_PRESSURED;
    }

    void start()
    {
        final int reproductionStreamId = configuration.reproductionStreamId();
        archiveScanningAgent = new FixArchiveScanningAgent(
            idleStrategy,
            DEFAULT_COMPACTION_SIZE,
            DEFAULT_MAXIMUM_BUFFER_SIZE,
            1, // TODO
            logFileDir,
            aeron,
            archive);

        final IntHashSet queryStreamIds = new IntHashSet();
        queryStreamIds.add(inboundLibraryStreamId);

        archiveScanningAgent.setup(
            aeronChannel, queryStreamIds, protocolHandler, null, false, reproductionStreamId);
    }

    long newConnectionId()
    {
        return protocolHandler.newConnectionId();
    }
}
