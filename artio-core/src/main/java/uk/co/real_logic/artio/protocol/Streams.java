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
package uk.co.real_logic.artio.protocol;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.Clock;
import uk.co.real_logic.artio.StreamInformation;
import uk.co.real_logic.artio.engine.RecordingCoordinator;

public final class Streams
{
    private final int streamId;
    private final Clock clock;
    private final Aeron aeron;
    private final String aeronChannel;
    private final boolean printAeronStreamIdentifiers;
    private final AtomicCounter failedPublications;
    private final int maxClaimAttempts;
    private final RecordingCoordinator recordingCoordinator;

    public Streams(
        final Aeron aeron,
        final String aeronChannel,
        final boolean printAeronStreamIdentifiers,
        final AtomicCounter failedPublications,
        final int streamId,
        final Clock clock,
        final int maxClaimAttempts,
        final RecordingCoordinator recordingCoordinator)
    {
        this.aeron = aeron;
        this.aeronChannel = aeronChannel;
        this.printAeronStreamIdentifiers = printAeronStreamIdentifiers;
        this.failedPublications = failedPublications;
        this.streamId = streamId;
        this.clock = clock;
        this.maxClaimAttempts = maxClaimAttempts;
        this.recordingCoordinator = recordingCoordinator;
    }

    public GatewayPublication gatewayPublication(
        final IdleStrategy idleStrategy, final ExclusivePublication dataPublication)
    {
        return new GatewayPublication(
            dataPublication,
            failedPublications,
            idleStrategy,
            clock,
            maxClaimAttempts);
    }

    public ExclusivePublication dataPublication(final String name)
    {
        final ExclusivePublication publication = recordingCoordinator.track(aeronChannel, streamId);
        StreamInformation.print(name, publication, printAeronStreamIdentifiers);
        return publication;
    }

    public Subscription subscription(final String name)
    {
        final Subscription subscription = aeron.addSubscription(aeronChannel, streamId);
        StreamInformation.print(name, subscription, printAeronStreamIdentifiers);
        return subscription;
    }
}
