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
package uk.co.real_logic.artio.library;

import io.aeron.Aeron;
import io.aeron.Subscription;
import org.agrona.concurrent.NanoClock;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.FixCounters;
import uk.co.real_logic.artio.StreamInformation;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.protocol.Streams;
import uk.co.real_logic.artio.replication.ClusterableStreams;

import static uk.co.real_logic.artio.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.LogTag.LIBRARY_CONNECT;

class LibraryTransport
{
    private final LibraryConfiguration configuration;
    private final FixCounters fixCounters;
    private final Aeron aeron;
    private final NanoClock nanoClock;

    private Subscription inboundSubscription;
    private GatewayPublication outboundPublication;

    LibraryTransport(
        final LibraryConfiguration configuration,
        final FixCounters fixCounters,
        final Aeron aeron)
    {
        this.configuration = configuration;
        this.fixCounters = fixCounters;
        this.aeron = aeron;
        this.nanoClock = configuration.nanoClock();
    }

    void initStreams(final String aeronChannel)
    {
        final ClusterableStreams soloNode = ClusterableStreams.solo(
            aeron, aeronChannel, configuration.printAeronStreamIdentifiers());
        DebugLogger.log(LIBRARY_CONNECT, "Directed streams at %s%n", aeronChannel);

        final Streams outboundLibraryStreams = new Streams(
            soloNode, fixCounters.failedOutboundPublications(), OUTBOUND_LIBRARY_STREAM, nanoClock,
            configuration.outboundMaxClaimAttempts());

        if (isReconnect())
        {
            inboundSubscription.close();
            outboundPublication.close();
        }
        inboundSubscription = aeron.addSubscription(aeronChannel, INBOUND_LIBRARY_STREAM);
        StreamInformation.print(
            "library " + configuration.libraryId() + " inboundSubscription", inboundSubscription, configuration);
        outboundPublication = outboundLibraryStreams.gatewayPublication(
            configuration.libraryIdleStrategy(), "outboundPublication");
    }

    Subscription inboundSubscription()
    {
        return inboundSubscription;
    }

    GatewayPublication outboundPublication()
    {
        return outboundPublication;
    }

    boolean isReconnect()
    {
        return inboundSubscription != null;
    }
}
