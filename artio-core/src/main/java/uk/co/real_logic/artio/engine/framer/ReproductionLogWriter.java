/*
 * Copyright 2022 Monotonic Ltd.
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
/*
 * Copyright 2022 Monotonic Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import io.aeron.ExclusivePublication;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.storage.messages.Bool;
import uk.co.real_logic.artio.storage.messages.ConnectionBackpressureEncoder;
import uk.co.real_logic.artio.storage.messages.MessageHeaderEncoder;

import static uk.co.real_logic.artio.engine.framer.TcpChannel.UNKNOWN_SEQ_NUM;
import static uk.co.real_logic.artio.storage.messages.MessageHeaderEncoder.ENCODED_LENGTH;

class ReproductionLogWriter
{
    private static final int CONN_BP_LEN = ENCODED_LENGTH + ConnectionBackpressureEncoder.BLOCK_LENGTH;

    private final ConnectionBackpressureEncoder connectionBackpressure = new ConnectionBackpressureEncoder();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[CONN_BP_LEN]);

    private final ExclusivePublication reproductionPublication;

    ReproductionLogWriter(final ExclusivePublication reproductionPublication)
    {
        this.reproductionPublication = reproductionPublication;

        // Just write a message so that the publication is created even if no bp occurs.
        // This simplifies the reading process as it ensures there's an image.
        while (reproductionPublication.offer(buffer) < 0)
        {
            Thread.yield();
        }

        final MessageHeaderEncoder messageHeader = new MessageHeaderEncoder();
        connectionBackpressure.wrapAndApplyHeader(buffer, 0, messageHeader);


    }

    void logBackPressure(final long connectionId, final int seqNum, final boolean replay, final int written)
    {
        if (seqNum == UNKNOWN_SEQ_NUM)
        {
            return;
        }

        connectionBackpressure
            .connectionId(connectionId)
            .sequenceNumber(seqNum)
            .isReplay(replay ? Bool.TRUE : Bool.FALSE)
            .written(written);

        // It's possible for us to drop these back-pressure entries but it's unlikely that this will happen
        // Reader code needs to cope with potentially missing entries from the log
        final ExclusivePublication reproductionPublication = this.reproductionPublication;
        final long position = reproductionPublication.offer(buffer, 0, CONN_BP_LEN);
        if (position < 0)
        {
            reproductionPublication.offer(buffer, 0, CONN_BP_LEN);
        }
    }
}
