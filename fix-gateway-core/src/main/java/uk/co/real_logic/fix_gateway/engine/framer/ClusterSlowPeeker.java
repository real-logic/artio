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
package uk.co.real_logic.fix_gateway.engine.framer;

import uk.co.real_logic.fix_gateway.replication.ClusterFragmentHandler;
import uk.co.real_logic.fix_gateway.replication.ClusterSubscription;

class ClusterSlowPeeker extends BlockablePosition
{
    private final ClusterSubscription normalClusterSubscription;
    private final ClusterSubscription peekClusterSubscription;

    ClusterSlowPeeker(
        final ClusterSubscription normalClusterSubscription,
        final ClusterSubscription peekClusterSubscription)
    {
        this.normalClusterSubscription = normalClusterSubscription;
        this.peekClusterSubscription = peekClusterSubscription;
    }

    int peek(final ClusterFragmentHandler handler)
    {
        blockPosition = DID_NOT_BLOCK;
        final long limitPosition = normalClusterSubscription.position();
        final long initialPosition = peekClusterSubscription.position();
        final long resultingPosition = peekClusterSubscription.peek(
                initialPosition, handler, limitPosition);
        final long delta = resultingPosition - initialPosition;
        peekClusterSubscription.position(blockPosition != DID_NOT_BLOCK ? blockPosition : resultingPosition);

        return (int) delta;
    }

}
