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
package uk.co.real_logic.fix_gateway;

import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;

/**
 * Frames data writes into sector aligned chunks with checksums at the end of each sector.
 */
public class SectorFramer
{
    public static final int OUT_OF_SPACE = -1;

    public static final int SECTOR_SIZE = 4096;
    public static final int CHECKSUM_SIZE = SIZE_OF_INT;
    public static final int SECTOR_DATA_LENGTH = SECTOR_SIZE - CHECKSUM_SIZE;
    public static final int FIRST_CHECKSUM_LOCATION = SECTOR_DATA_LENGTH;

    private final int capacity;

    private int checksumOffset;
    private int sectorStart;

    public SectorFramer(final int capacity)
    {
        this.capacity = capacity;
    }

    public int claim(int filePosition, final int length)
    {
        final int nextSectorStart = nextSectorStart(filePosition);
        checksumOffset = nextSectorStart - CHECKSUM_SIZE;
        final int proposedRecordEnd = filePosition + length;
        // If the data would span the end of a sector then
        if (proposedRecordEnd > checksumOffset)
        {
            filePosition = nextSectorStart;
            checksumOffset += SECTOR_SIZE;
        }

        sectorStart = nextSectorStart - SECTOR_SIZE;

        return (filePosition + length) <= capacity ? filePosition : OUT_OF_SPACE;
    }

    public int checksumOffset()
    {
        return checksumOffset;
    }

    public int sectorStart()
    {
        return sectorStart;
    }

    private static int nextSectorStart(final int offset)
    {
        return ((offset / SECTOR_SIZE) * SECTOR_SIZE) + SECTOR_SIZE;
    }

    public static void validateCheckSum(
        final int start,
        final int end,
        final int calculateChecksum,
        final int savedChecksum,
        final String fileName)
    {
        if (calculateChecksum != savedChecksum)
        {
            throw new FileSystemCorruptionException(
                String.format(
                    "The " + fileName + " file is corrupted between bytes %d and %d, " +
                    "saved checksum is %d, but %d was calculated",
                    start,
                    end,
                    savedChecksum,
                    calculateChecksum));
        }
    }
}
