/*
 * Copyright 2022 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import org.agrona.collections.Long2ObjectHashMap;

import static uk.co.real_logic.artio.engine.logger.ReplayQuery.trueBeginPosition;

class StartPositionQuery
{
    private final Long2ObjectHashMap<PrunePosition> recordingIdToStartPosition = new Long2ObjectHashMap<>();

    private int highestSequenceIndex = 0;

    public StartPositionQuery()
    {
    }

    // Looking for the highest position of the lowest sequence number entry of the highest sequence index
    public void updateStartPosition(
        final int sequenceNumber, final int sequenceIndex, final long recordingId, final long beginPosition)
    {
        final long trueBeginPosition = trueBeginPosition(beginPosition);

        if (sequenceIndex > highestSequenceIndex)
        {
            // Don't want the lower positions of a previous sequence index to matter.
            recordingIdToStartPosition.clear();
            recordingIdToStartPosition.put(recordingId,
                new PrunePosition(trueBeginPosition, sequenceNumber, sequenceIndex));
            highestSequenceIndex = sequenceIndex;

        }
        else if (sequenceIndex == highestSequenceIndex)
        {
            // Might have other messages on different recording ids
            final PrunePosition oldPosition = recordingIdToStartPosition.get(recordingId);
            if (oldPosition == null)
            {
                recordingIdToStartPosition.put(recordingId,
                    new PrunePosition(trueBeginPosition, sequenceNumber, sequenceIndex));
            }
            else
            {
                final int oldSequenceNumber = oldPosition.sequenceNumber();
                if (oldSequenceNumber == sequenceNumber)
                {
                    oldPosition.position(Math.max(oldPosition.position(), trueBeginPosition));
                }
                else if (sequenceNumber < oldSequenceNumber)
                {
                    oldPosition.sequenceNumber(sequenceNumber);
                    oldPosition.position(trueBeginPosition);
                }
            }
        }
    }

    public int highestSequenceIndex()
    {
        return highestSequenceIndex;
    }

    public Long2ObjectHashMap<PrunePosition> recordingIdToStartPosition()
    {
        return recordingIdToStartPosition;
    }
}
