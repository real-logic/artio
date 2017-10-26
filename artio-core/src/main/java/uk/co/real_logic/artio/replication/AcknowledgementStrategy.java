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
package uk.co.real_logic.artio.replication;

import org.agrona.collections.Long2LongHashMap;

/**
 * Implement this to determine what position to acknowledge. Most
 * systems should use a Quorum based acknowledgement. Some systems
 * may want to ensure that the entire cluster has acknowledged a
 * position. You can also implement a custom strategy.
 */
public interface AcknowledgementStrategy
{
    static AcknowledgementStrategy quorum()
    {
        return new QuorumAcknowledgementStrategy();
    }

    static AcknowledgementStrategy entireCluster()
    {
        return new EntireClusterAcknowledgementStrategy();
    }

    long findAckedTerm(Long2LongHashMap sessionIdToPosition);

    boolean isElected(int receivedVotes, int clusterSize);
}
