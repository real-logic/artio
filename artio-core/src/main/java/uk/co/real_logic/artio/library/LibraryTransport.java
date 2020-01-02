/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.library;

import io.aeron.Aeron;
import io.aeron.Subscription;
import uk.co.real_logic.artio.Clock;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.FixCounters;
import uk.co.real_logic.artio.StreamInformation;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.protocol.Streams;

import static uk.co.real_logic.artio.LogTag.LIBRARY_CONNECT;

class LibraryTransport
{
    private final LibraryConfiguration configuration;
    private final FixCounters fixCounters;
    private final Aeron aeron;
    private final Clock clock;

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
        this.clock = configuration.clock();
    }

    void initStreams(final String aeronChannel)
    {
        DebugLogger.log(LIBRARY_CONNECT, "Directed streams at %s%n", aeronChannel);

        final Streams outboundLibraryStreams = new Streams(
            aeron,
            aeronChannel,
            configuration.printAeronStreamIdentifiers(),
            fixCounters.failedOutboundPublications(),
            configuration.outboundLibraryStream(),
            clock,
            configuration.outboundMaxClaimAttempts(),
            null);

        if (isReconnect())
        {
            inboundSubscription.close();
            outboundPublication.close();
        }

        inboundSubscription = aeron.addSubscription(aeronChannel, configuration.inboundLibraryStream());
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
