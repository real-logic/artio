/*
 * Copyright 2015 Real Logic Ltd.
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

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.fix_gateway.engine.framer.ReliefValve;

/**
 * .
 */
public class RaftTransport
{
    private final RaftNodeConfiguration configuration;

    public RaftTransport(final RaftNodeConfiguration configuration)
    {
        this.configuration = configuration;
    }

    public void initialiseRoles(final Leader leader, final Candidate candidate, final Follower follower)
    {
        final RaftPublication acknowledgementPublication = raftPublication(configuration.acknowledgementStream());
        final RaftPublication controlPublication = raftPublication(configuration.controlStream());

        leader
            .controlPublication(controlPublication);

        candidate
            .controlPublication(controlPublication);

        follower
            .controlPublication(controlPublication)
            .acknowledgementPublication(acknowledgementPublication);
    }

    private RaftPublication raftPublication(final StreamIdentifier id)
    {
        return new RaftPublication(
            configuration.maxClaimAttempts(),
            configuration.idleStrategy(),
            configuration.failCounter(),
            ReliefValve.NONE,
            publication(id));
    }

    public void injectLeaderSubscriptions(final Leader leader)
    {
        leader
            .acknowledgementSubscription(subscription(configuration.acknowledgementStream()))
            .dataSubscription(subscription(configuration.dataStream()));
    }

    public void injectCandidateSubscriptions(final Candidate candidate)
    {
        candidate
            .controlSubscription(subscription(configuration.controlStream()));
    }

    public void injectFollowerSubscriptions(final Follower follower)
    {
        configuration.archiver()
            .subscription(subscription(configuration.dataStream()));

        follower
            .controlSubscription(subscription(configuration.controlStream()));
    }


    private Publication publication(final StreamIdentifier id)
    {
        return configuration.aeron().addPublication(id.channel(), id.streamId());
    }

    private Subscription subscription(final StreamIdentifier id)
    {
        return configuration.aeron().addSubscription(id.channel(), id.streamId());
    }

}
