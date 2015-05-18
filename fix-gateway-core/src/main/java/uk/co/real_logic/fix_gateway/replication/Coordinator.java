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

import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.agrona.collections.Long2LongHashMap;

public class Coordinator implements ControlProtocol
{
    public static final int NO_SESSION_ID = -1;

    private final TermAcknowledgementStrategy termAcknowledgementStrategy;
    private final GatewaySubscription subscription;

    // Counts of how many acknowledgements
    private final Long2LongHashMap nodeToAckedPosition = new Long2LongHashMap(NO_SESSION_ID);
    private long acknowledgedTerm = 0;

    public Coordinator(
        final TermAcknowledgementStrategy termAcknowledgementStrategy,
        final IntHashSet followers,
        final GatewaySubscription subscription)
    {
        this.termAcknowledgementStrategy = termAcknowledgementStrategy;
        this.subscription = subscription;
        followers.forEach(follower -> nodeToAckedPosition.put(follower, 0));
    }

    public void messageAcknowledgement(final long newAckedPosition, final int node)
    {
        final long lastAckedPosition = nodeToAckedPosition.get(node);
        if (lastAckedPosition != NO_SESSION_ID)
        {
            if (newAckedPosition > lastAckedPosition)
            {
                nodeToAckedPosition.put(node, newAckedPosition);

                final long newAcknowledgedTerm = termAcknowledgementStrategy.findAckedTerm(nodeToAckedPosition);
                if (newAcknowledgedTerm > acknowledgedTerm)
                {
                    subscription.pollToPosition(newAckedPosition);
                    acknowledgedTerm = newAcknowledgedTerm;
                }
            }
        }
        else
        {
            // TODO: error case
        }
    }

}
