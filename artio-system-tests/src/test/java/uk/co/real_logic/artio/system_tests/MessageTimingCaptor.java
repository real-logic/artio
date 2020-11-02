/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.system_tests;

import org.agrona.collections.LongArrayList;
import uk.co.real_logic.artio.engine.MessageTimingHandler;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

public class MessageTimingCaptor implements MessageTimingHandler
{

    private final LongArrayList sequenceNumbers = new LongArrayList();

    public void onMessage(final long sequenceNumber, final long connectionId)
    {
        sequenceNumbers.add(sequenceNumber);
    }

    public void verifyConsecutiveSequenceNumbers(final int lastSentMsgSeqNum)
    {
        assertThat(sequenceNumbers, hasSize(lastSentMsgSeqNum));
        for (int i = 0; i < lastSentMsgSeqNum; i++)
        {
            final long sequenceNumber = sequenceNumbers.getLong(i);
            assertEquals(i + 1L, sequenceNumber);
        }
    }

}
