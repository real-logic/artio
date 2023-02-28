/*
 * Copyright 2015-2023 Real Logic Limited.
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
package uk.co.real_logic.artio.engine;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.AtomicBuffer;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

// TODO: optimisation: write and checksum only needed bytes
public class ChecksumFramer extends SectorFramer
{
    private final CRC32 crc32 = new CRC32();
    private final AtomicBuffer buffer;
    private final ChecksumConsumer saveChecksumFunc;
    private final ErrorHandler errorHandler;
    private final int errorReportingOffset;
    private final ChecksumConsumer validateChecksumFunc;
    private final String fileName;
    private final boolean indexChecksumEnabled;

    public ChecksumFramer(
        final AtomicBuffer buffer,
        final int capacity,
        final ErrorHandler errorHandler,
        final int errorReportingOffset,
        final String fileName,
        final boolean indexChecksumEnabled)
    {
        super(capacity);
        this.buffer = buffer;
        saveChecksumFunc = buffer::putInt;
        this.errorHandler = errorHandler;
        this.errorReportingOffset = errorReportingOffset;
        this.fileName = fileName;
        this.indexChecksumEnabled = indexChecksumEnabled;
        validateChecksumFunc = this::validateChecksum;
    }

    public void validateCheckSums()
    {
        if (indexChecksumEnabled)
        {
            withChecksums(validateChecksumFunc);
        }
    }

    public void updateChecksums()
    {
        if (indexChecksumEnabled)
        {
            withChecksums(saveChecksumFunc);
        }
    }

    private void validateChecksum(final int checksumOffset, final int calculatedChecksum)
    {
        final int savedChecksum = buffer.getInt(checksumOffset);
        final int start = errorReportingOffset + checksumOffset - SECTOR_DATA_LENGTH;
        final int end = errorReportingOffset + checksumOffset + CHECKSUM_SIZE;
        validateCheckSum(fileName, start, end, savedChecksum, calculatedChecksum, errorHandler);
    }

    private void withChecksums(final ChecksumConsumer consumer)
    {
        final byte[] inMemoryBytes = buffer.byteArray();
        final ByteBuffer inMemoryByteBuffer = buffer.byteBuffer();
        final int wrapAdjustment = buffer.wrapAdjustment();
        final int capacity = this.capacity;

        for (int sectorEnd = SECTOR_SIZE; sectorEnd <= capacity; sectorEnd += SECTOR_SIZE)
        {
            final int sectorStart = sectorEnd - SECTOR_SIZE + wrapAdjustment;
            final int checksumOffset = sectorEnd - CHECKSUM_SIZE;

            crc32.reset();
            if (inMemoryBytes != null)
            {
                crc32.update(inMemoryBytes, sectorStart, SECTOR_DATA_LENGTH);
            }
            else
            {
                ByteBufferUtil.limit(inMemoryByteBuffer, sectorStart + SECTOR_DATA_LENGTH);
                ByteBufferUtil.position(inMemoryByteBuffer, sectorStart);
                crc32.update(inMemoryByteBuffer);
            }
            final int sectorChecksum = (int)crc32.getValue();
            consumer.accept(checksumOffset, sectorChecksum);
        }

        if (inMemoryByteBuffer != null)
        {
            inMemoryByteBuffer.clear();
        }
    }

    private interface ChecksumConsumer
    {
        void accept(int checksumOffset, int sectorChecksum);
    }
}
