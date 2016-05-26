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

import io.aeron.Publication;
import io.aeron.Subscription;
import uk.co.real_logic.fix_gateway.ReliefValve;

public class RaftTransport
{
    private final ClusterNodeConfiguration configuration;

    public RaftTransport(final ClusterNodeConfiguration configuration)
    {
        this.configuration = configuration;
    }

    public void initialiseRoles(final Leader leader, final Candidate candidate, final Follower follower)
    {
        final RaftPublication acknowledgementPublication = raftPublication(configuration.acknowledgementStream());
        final RaftPublication controlPublication = raftPublication(configuration.controlStream());
        final Subscription controlSubscription = subscription(configuration.controlStream());

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

    public void injectLeaderSubscriptions(final Leader leader)
    {
        leader
            .acknowledgementSubscription(subscription(configuration.acknowledgementStream()))
            .dataSubscription(subscription(configuration.dataStream()));
    }

    public void injectFollowerSubscriptions(final Follower follower)
    {
        configuration.archiver()
                     .subscription(subscription(configuration.dataStream()));
    }

    private Publication publication(final StreamIdentifier id)
    {
        return configuration.aeron().addPublication(id.channel(), id.streamId());
    }

    private Subscription subscription(final StreamIdentifier id)
    {
        return configuration.aeron().addSubscription(id.channel(), id.streamId());
    }

    private RaftPublication raftPublication(final StreamIdentifier id)
    {
        return new RaftPublication(
            configuration.maxClaimAttempts(),
            configuration.idleStrategy(),
            configuration.failCounter(),
            ReliefValve.NO_RELIEF_VALVE,
            publication(id));
    }

    public Publication leaderPublication()
    {
        return publication(configuration.dataStream());
    }
}
