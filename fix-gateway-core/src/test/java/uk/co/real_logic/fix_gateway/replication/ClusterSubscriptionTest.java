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

import io.aeron.Image;
import io.aeron.Subscription;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.hasResult;

/**
 * Test technically breaches encapsulation of ClusterSubscription
 * deliberate tradeoff to avoid additional indirection and test complexity.
 */
public class ClusterSubscriptionTest
{
    private static final int CLUSTER_STREAM_ID = 1;

    private Subscription dataSubscription = mock(Subscription.class);
    private Subscription controlSubscription = mock(Subscription.class);
    private Image dataImage = mock(Image.class);
    private ArgumentCaptor<Integer> leadershipSessionId = ArgumentCaptor.forClass(Integer.class);

    private ClusterSubscription clusterSubscription = new ClusterSubscription(
        dataSubscription, CLUSTER_STREAM_ID, controlSubscription);

    @Before
    public void setUp()
    {
        when(dataSubscription.imageBySessionId(leadershipSessionId.capture())).thenReturn(dataImage);
    }

    @Test
    public void shouldUpdatePositionWhenAcknowledged()
    {
        onConsensusHeartbeatPoll(1, 1, 1, 0);

        onConsensusHeartbeatPoll(1, 1, 2, 1);

        assertState(1, 1, 2);
    }

    @Test
    public void shouldStashUpdatesWithGap()
    {
        onConsensusHeartbeatPoll(1, 1, 1, 0);

        onConsensusHeartbeatPoll(2, 2, 4, 2);

        assertState(1, 1, 1);
    }

    @Test
    public void shouldApplyUpdatesWhenGapFilled()
    {
        shouldStashUpdatesWithGap();

        onConsensusHeartbeatPoll(1, 1, 2, 1);

        assertState(1, 1, 2);

        clusterSubscription.hasMatchingFutureAck();

        assertState(2, 2, 4);
    }

    @Test
    public void shouldStashUpdatesFromFutureLeadershipTerm()
    {
        onConsensusHeartbeatPoll(1, 1, 1, 0);

        onConsensusHeartbeatPoll(3, 3, 4, 2);

        assertState(1, 1, 1);
    }

    @Test
    public void shouldApplyUpdatesFromFutureLeadershipTerm()
    {
        shouldStashUpdatesFromFutureLeadershipTerm();

        onConsensusHeartbeatPoll(2, 2, 2, 1);

        assertState(2, 2, 2);

        clusterSubscription.hasMatchingFutureAck();

        assertState(3, 3, 4);
    }

    private void onConsensusHeartbeatPoll(
        final int leaderShipTermId,
        final int leaderSessionId,
        final int position,
        final int previousPosition)
    {
        clusterSubscription.hasMatchingFutureAck();
        clusterSubscription.onConsensusHeartbeat(leaderShipTermId, leaderSessionId, position, previousPosition);
    }

    private void assertState(
        final int currentLeadershipTermId,
        final Integer leadershipSessionId,
        final long currentConsensusPosition)
    {
        assertThat(clusterSubscription,
            hasResult(
                "currentLeadershipTermId",
                ClusterSubscription::currentLeadershipTermId,
                equalTo(currentLeadershipTermId)));

        assertEquals("Wrong leadershipSessionId", leadershipSessionId, this.leadershipSessionId.getValue());

        assertThat(clusterSubscription,
            hasResult(
                "currentConsensusPosition",
                ClusterSubscription::currentConsensusPosition,
                equalTo(currentConsensusPosition)));
    }
}
