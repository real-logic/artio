/*
 * Copyright 2015-2016 Real Logic Ltd.
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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReservedValueTest
{
    @Test
    public void shouldStoreChecksumAndStreamIdIndependently()
    {
        final int streamId = 0;
        final int checksum = 0xFFFF;
        final long reservedValue = ReservedValue.of(streamId, checksum);

        assertEquals(streamId, ReservedValue.streamId(reservedValue));
        assertEquals(checksum, ReservedValue.checksum(reservedValue));
    }

    @Test
    public void shouldStoreStreamId()
    {
        final int streamId = 0xFFFF;
        final int checksum = 0;
        final long reservedValue = ReservedValue.ofStreamId(streamId);

        assertEquals(streamId, ReservedValue.streamId(reservedValue));
        assertEquals(checksum, ReservedValue.checksum(reservedValue));
    }

    @Test
    public void shouldStoreChecksum()
    {
        final int streamId = 0;
        final int checksum = 0xFFFF;
        final long reservedValue = ReservedValue.ofChecksum(checksum);

        assertEquals(streamId, ReservedValue.streamId(reservedValue));
        assertEquals(checksum, ReservedValue.checksum(reservedValue));
    }
}
