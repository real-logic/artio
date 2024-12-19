/*
 * Copyright 2021 Adaptive Financial Consulting Ltd.
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

import io.aeron.ExclusivePublication;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;
import uk.co.real_logic.artio.messages.ReplayerTimestampDecoder;
import uk.co.real_logic.artio.messages.ReplayerTimestampEncoder;

import static java.util.concurrent.TimeUnit.SECONDS;
import static uk.co.real_logic.artio.messages.MessageHeaderDecoder.ENCODED_LENGTH;

class ReplayTimestamper
{
    private static final long TIMESTAMP_MESSAGE_INTERVAL = SECONDS.toNanos(2);

    private final UnsafeBuffer timestampBuffer = new UnsafeBuffer(new byte[
        ENCODED_LENGTH + ReplayerTimestampDecoder.BLOCK_LENGTH]);
    private final ReplayerTimestampEncoder replayerTimestampEncoder = new ReplayerTimestampEncoder();

    private final EpochNanoClock clock;

    private final ExclusivePublication publication;
    private long nextTimestampMessageInNs;

    ReplayTimestamper(final ExclusivePublication publication, final EpochNanoClock clock)
    {
        this.publication = publication;
        this.clock = clock;
        updateTimestamp(clock.nanoTime());

        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        replayerTimestampEncoder.wrapAndApplyHeader(timestampBuffer, 0, messageHeaderEncoder);
    }

    void sendTimestampMessage(final long timeInNs)
    {
        if (timeInNs > nextTimestampMessageInNs)
        {
            replayerTimestampEncoder.timestamp(timeInNs);
            final long position = publication.offer(timestampBuffer);
            if (position > 0)
            {
                updateTimestamp(timeInNs);
            }
        }
    }

    private void updateTimestamp(final long timeInNs)
    {
        nextTimestampMessageInNs = timeInNs + TIMESTAMP_MESSAGE_INTERVAL;
    }
}
