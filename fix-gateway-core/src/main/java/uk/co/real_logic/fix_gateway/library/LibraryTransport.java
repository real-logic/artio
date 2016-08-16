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
package uk.co.real_logic.fix_gateway.library;

import io.aeron.Aeron;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.replication.ClusterableStreams;
import uk.co.real_logic.fix_gateway.replication.ClusterableSubscription;

import static uk.co.real_logic.fix_gateway.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.LogTag.LIBRARY_CONNECT;

class LibraryTransport
{

    private final LibraryConfiguration configuration;
    private final FixCounters fixCounters;
    private final Aeron aeron;

    private Streams inboundLibraryStreams;
    private Streams outboundLibraryStreams;

    private ClusterableSubscription inboundSubscription;
    private GatewayPublication outboundPublication;

    LibraryTransport(
        final LibraryConfiguration configuration,
        final FixCounters fixCounters,
        final Aeron aeron)
    {
        this.configuration = configuration;
        this.fixCounters = fixCounters;
        this.aeron = aeron;
    }

    void initStreams(final String aeronChannel)
    {
        final NanoClock nanoClock = new SystemNanoClock();
        final ClusterableStreams soloNode = ClusterableStreams.solo(aeron, aeronChannel);
        DebugLogger.log(LIBRARY_CONNECT, "Attempting connect to %s\n", aeronChannel);

        inboundLibraryStreams = new Streams(
            soloNode, fixCounters.failedInboundPublications(), INBOUND_LIBRARY_STREAM, nanoClock,
            configuration.inboundMaxClaimAttempts());
        outboundLibraryStreams = new Streams(
            soloNode, fixCounters.failedOutboundPublications(), OUTBOUND_LIBRARY_STREAM, nanoClock,
            configuration.outboundMaxClaimAttempts());


        if (isReconnect())
        {
            inboundSubscription.close();
            outboundPublication.close();
        }
        inboundSubscription = inboundLibraryStreams.subscription();
        outboundPublication = outboundLibraryStreams.gatewayPublication(configuration.libraryIdleStrategy());
    }

    ClusterableSubscription inboundSubscription()
    {
        return inboundSubscription;
    }

    GatewayPublication outboundPublication()
    {
        return outboundPublication;
    }

    private boolean isReconnect()
    {
        return inboundSubscription != null;
    }
}
