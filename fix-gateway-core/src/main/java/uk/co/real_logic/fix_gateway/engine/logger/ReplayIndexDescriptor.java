/*
 * Copyright 2015-2017 Real Logic Ltd.
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

import org.agrona.BitUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;

import java.io.File;

public class ReplayIndexDescriptor
{

    static final int REPLAY_POSITION_BUFFER_SIZE = 128 * 1024;

    static final int BEGIN_CHANGE_OFFSET = MessageHeaderEncoder.ENCODED_LENGTH;
    static final int END_CHANGE_OFFSET = BEGIN_CHANGE_OFFSET + BitUtil.SIZE_OF_INT;
    public static final int INITIAL_RECORD_OFFSET = END_CHANGE_OFFSET + BitUtil.SIZE_OF_INT;

    static final int RECORD_LENGTH = 32;

    static File logFile(final String logFileDir, final long fixSessionId, final int streamId)
    {
        return new File(String.format(logFileDir + File.separator + "replay-index-%d-%d", fixSessionId, streamId));
    }

    public static UnsafeBuffer replayPositionBuffer(final String logFileDir, final int streamId)
    {
        final String pathname = replayPositionPath(logFileDir, streamId);
        return new UnsafeBuffer(LoggerUtil.map(new File(pathname), REPLAY_POSITION_BUFFER_SIZE));
    }

    static String replayPositionPath(final String logFileDir, final int streamId)
    {
        return logFileDir + File.separator + "replay-positions-"  + streamId;
    }

    static void endChangeVolatile(final AtomicBuffer buffer, final int changeNumber)
    {
        buffer.putIntVolatile(END_CHANGE_OFFSET, changeNumber);
    }

    static int endChangeVolatile(final AtomicBuffer buffer)
    {
        return buffer.getIntVolatile(END_CHANGE_OFFSET);
    }

    static void beginChangeVolatile(final AtomicBuffer buffer, final int changeNumber)
    {
        buffer.putIntVolatile(BEGIN_CHANGE_OFFSET, changeNumber);
    }

    static int beginChangeVolatile(final AtomicBuffer buffer)
    {
        return buffer.getIntVolatile(BEGIN_CHANGE_OFFSET);
    }

    static int recordCapacity(final int indexFileSize)
    {
        return indexFileSize - INITIAL_RECORD_OFFSET;
    }

    static int offset(final int changePosition, final int capacity)
    {
        return INITIAL_RECORD_OFFSET + (changePosition & (capacity - 1));
    }

    static void checkIndexFileSize(final int indexFileSize)
    {
        if (!BitUtil.isPowerOfTwo(recordCapacity(indexFileSize)))
        {
            throw new IllegalStateException(
                "IndexFileSize must be a positive power of 2 + INITIAL_RECORD_OFFSET: indexFileSize=" + indexFileSize);
        }
    }
}
