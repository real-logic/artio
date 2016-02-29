/*
 * Copyright 2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine;

import uk.co.real_logic.agrona.concurrent.AtomicBuffer;

import java.util.zip.CRC32;

// TODO: optimisation: write and checksum only needed bytes
public class ChecksumFramer extends SectorFramer
{
    private final CRC32 crc32 = new CRC32();
    private final AtomicBuffer buffer;
    private final ChecksumConsumer saveChecksumFunc;
    private final ChecksumConsumer validateChecksumFunc;

    public ChecksumFramer(final AtomicBuffer buffer, final int capacity)
    {
        super(capacity);
        this.buffer = buffer;
        saveChecksumFunc = buffer::putInt;
        validateChecksumFunc = this::validateChecksum;
    }

    public void validateCheckSums()
    {
        withChecksums(validateChecksumFunc);
    }

    public void updateChecksums()
    {
        withChecksums(saveChecksumFunc);
    }

    private void validateChecksum(final int checksumOffset, final int calculatedChecksum)
    {
        final int savedChecksum = buffer.getInt(checksumOffset);
        final int start = checksumOffset - SECTOR_DATA_LENGTH;
        final int end = checksumOffset + CHECKSUM_SIZE;
        validateCheckSum(start, end, calculatedChecksum, savedChecksum, "sequence numbers");
    }

    private void withChecksums(final ChecksumConsumer consumer)
    {
        final byte[] inMemoryBytes = buffer.byteArray();
        final int capacity = this.capacity;

        for (int sectorEnd = SECTOR_SIZE; sectorEnd <= capacity; sectorEnd += SECTOR_SIZE)
        {
            final int sectorStart = sectorEnd - SECTOR_SIZE;
            final int checksumOffset = sectorEnd - CHECKSUM_SIZE;

            crc32.reset();
            crc32.update(inMemoryBytes, sectorStart, SECTOR_DATA_LENGTH);
            final int sectorChecksum = (int) crc32.getValue();
            consumer.accept(checksumOffset, sectorChecksum);
        }
    }

    private interface ChecksumConsumer
    {
        void accept(int checksumOffset, int sectorChecksum);
    }
}
