/*
 * Copyright 2015=2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine.logger;

import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.LastKnownSequenceNumberDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;

import java.io.File;

import static uk.co.real_logic.fix_gateway.engine.SectorFramer.SECTOR_SIZE;
import static uk.co.real_logic.fix_gateway.engine.SectorFramer.nextSectorStart;

/**
 * Stores a cache of the last sent sequence number.
 * <p>
 * Each instance is not thread-safe, however, they can share a common
 * off-heap in a single-writer threadsafe manner.
 *
 * Layout:
 *
 * Message Header
 * Known Stream Position
 * Series of LastKnownSequenceNumber records
 */
final class SequenceNumberIndexDescriptor
{
    static final int HEADER_SIZE = MessageHeaderDecoder.ENCODED_LENGTH;
    static final int RECORD_SIZE = LastKnownSequenceNumberDecoder.BLOCK_LENGTH;

    static final double SEQUENCE_NUMBER_RATIO = 0.9;

    static AtomicBuffer positionsBuffer(final AtomicBuffer buffer, final int positionsOffset)
    {
        return new UnsafeBuffer(buffer, positionsOffset, buffer.capacity() - positionsOffset);
    }

    /**
     * Calculated an offset in the sequence number index for storing positions.
     * This is the sector aligned location, closest to SEQUENCE_NUMBER_RATIO * fileCapacity.
     *
     * @param fileCapacity the capacity of the overall table
     * @return an offset in the sequence number index for storing positions.
     */
    static int positionTableOffset(final int fileCapacity)
    {
        final int proposedCapacity = nextSectorStart((int) (fileCapacity * SEQUENCE_NUMBER_RATIO));
        if (proposedCapacity == fileCapacity)
        {
            return fileCapacity - SECTOR_SIZE;
        }
        return proposedCapacity;
    }

    public static File passingPath(String indexFilePath)
    {
        return new File(indexFilePath + "-passing");
    }

    public static File writablePath(String indexFilePath)
    {
        return new File(indexFilePath + "-writable");
    }
}
