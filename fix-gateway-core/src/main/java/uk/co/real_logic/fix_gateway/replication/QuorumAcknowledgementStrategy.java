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

import uk.co.real_logic.agrona.collections.Long2LongHashMap;

import java.util.Arrays;

/**
 * A leaderShipTerm is acknowledged if a quorum of cluster members acknowledge it
 */
public class QuorumAcknowledgementStrategy implements AcknowledgementStrategy
{
    private long[] positions = new long[0];

    @Override
    public long findAckedTerm(final Long2LongHashMap sessionIdToPosition)
    {
        final int size = sessionIdToPosition.size();
        if (size == 0)
        {
            return 0;
        }

        if (size == 1)
        {
            return sessionIdToPosition.values().iterator().next();
        }

        final long[] positions = copyPositions(sessionIdToPosition, size);

        Arrays.sort(positions);

        final int quorumPoint = (size % 2 == 0) ? size / 2 - 1 : size / 2;

        return positions[quorumPoint];
    }

    private long[] copyPositions(final Long2LongHashMap sessionIdToPosition, final int size)
    {
        long[] positions = this.positions;
        if (positions.length != size)
        {
            this.positions = positions = new long[size];
        }

        // TODO: remove cast on next agrona release:
        final Long2LongHashMap.Values values = (Long2LongHashMap.Values) sessionIdToPosition.values();
        final Long2LongHashMap.LongIterator it = values.iterator();
        for (int i = 0; i < size; i++)
        {
            positions[i] = it.nextValue();
        }
        return positions;
    }
}
