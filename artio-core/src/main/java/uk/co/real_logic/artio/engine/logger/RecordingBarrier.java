/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.CountersReader;

public class RecordingBarrier
{
    private final AeronArchive aeronArchive;

    public RecordingBarrier(final AeronArchive aeronArchive)
    {
        this.aeronArchive = aeronArchive;
    }

    void await(final long recordingId, final long reachedPosition, final IdleStrategy idleStrategy)
    {
        final CountersReader countersReader = aeronArchive.context().aeron().countersReader();
        final int counterId = RecordingPos.findCounterIdByRecording(
            countersReader,
            recordingId);

        // wait if the recording is active - otherwise assume that the recording has complete.
        if (counterId != CountersReader.NULL_COUNTER_ID)
        {
            long recordedPosition;
            while ((recordedPosition = countersReader.getCounterValue(counterId)) < reachedPosition)
            {
                idleStrategy.idle();

                if (!RecordingPos.isActive(countersReader, counterId, recordingId))
                {
                    return;
                }
            }
            idleStrategy.reset();
        }
    }
}
