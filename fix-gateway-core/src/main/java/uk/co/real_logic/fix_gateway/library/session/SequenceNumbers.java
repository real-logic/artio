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
package uk.co.real_logic.fix_gateway.library.session;

import uk.co.real_logic.agrona.collections.Long2LongHashMap;

public class SequenceNumbers
{
    public static final int MISSING = -1;

    private final Long2LongHashMap sessionIdToLastSentSequenceNumber = new Long2LongHashMap(MISSING);
    private final boolean acceptorSequenceNumbersResetUponReconnect;

    public SequenceNumbers(final boolean acceptorSequenceNumbersResetUponReconnect)
    {
        this.acceptorSequenceNumbersResetUponReconnect = acceptorSequenceNumbersResetUponReconnect;
    }

    public int onInitiate(final long sessionId)
    {
        return remove(sessionId);
    }

    public int onAccept(final long sessionId)
    {
        if (acceptorSequenceNumbersResetUponReconnect)
        {
            return 1;
        }

        return remove(sessionId);
    }

    private int remove(final long sessionId)
    {
        return (int) sessionIdToLastSentSequenceNumber.remove(sessionId);
    }

    public void onDisconnect(final long sessionId, final int sequenceNumber)
    {
        sessionIdToLastSentSequenceNumber.put(sessionId, sequenceNumber);
    }
}
