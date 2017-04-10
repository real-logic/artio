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
package uk.co.real_logic.fix_gateway.replication;

import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import uk.co.real_logic.fix_gateway.StreamInformation;

class RaftTransport
{
    private final ClusterNodeConfiguration configuration;

    RaftTransport(final ClusterNodeConfiguration configuration)
    {
        this.configuration = configuration;
    }

    void initialiseRoles(final Leader leader, final Candidate candidate, final Follower follower)
    {
        final RaftPublication acknowledgementPublication = raftPublication(
            configuration.acknowledgementStream(), "acknowledgementPublication");
        final StreamIdentifier controlStream = configuration.controlStream();
        final RaftPublication controlPublication = raftPublication(controlStream, "controlStream");
        final Subscription controlSubscription = controlSubscription();

        leader
            .controlPublication(controlPublication)
            .controlSubscription(controlSubscription);

        candidate
            .controlPublication(controlPublication)
            .controlSubscription(controlSubscription);

        follower
            .controlPublication(controlPublication)
            .acknowledgementPublication(acknowledgementPublication)
            .controlSubscription(controlSubscription);
    }

    void injectLeaderSubscriptions(final Leader leader)
    {
        final StreamIdentifier data = configuration.dataStream();
        final StreamIdentifier acknowledgement = configuration.acknowledgementStream();
        leader
            .acknowledgementSubscription(
                subscription(
                    acknowledgement.channel(), acknowledgement.streamId(), "leaderAcknowledgementSubscription"))
            .dataSubscription(
                subscription(
                    data.spyChannel(), data.streamId(), "leaderDataSubscription"));
    }

    Subscription dataSubscription()
    {
        final StreamIdentifier dataStream = configuration.dataStream();
        return subscription(dataStream.channel(), dataStream.streamId(), "dataSubscription");
    }

    Subscription controlSubscription()
    {
        final StreamIdentifier controlStream = configuration.controlStream();
        return subscription(controlStream.channel(), controlStream.streamId(), "controlSubscription");
    }

    void injectFollowerSubscriptions(final Follower follower)
    {
        follower.dataSubscription(dataSubscription());
    }

    private ExclusivePublication publication(final StreamIdentifier id, final String name)
    {
        final ExclusivePublication publication = configuration
            .aeron()
            .addExclusivePublication(id.channel(), id.streamId());
        StreamInformation.print(name, publication, configuration.printAeronStreamIdentifiers());
        return publication;
    }

    private Subscription subscription(final String channel, final int streamId, final String name)
    {
        final Subscription subscription = configuration
            .aeron()
            .addSubscription(channel, streamId);
        StreamInformation.print(name, subscription, configuration.printAeronStreamIdentifiers());
        return subscription;
    }

    private RaftPublication raftPublication(final StreamIdentifier id, final String name)
    {
        return new RaftPublication(
            configuration.maxClaimAttempts(),
            configuration.idleStrategy(),
            configuration.failCounter(),
            publication(id, name));
    }

    ExclusivePublication leaderPublication()
    {
        return publication(configuration.dataStream(), "leaderPublication");
    }
}
