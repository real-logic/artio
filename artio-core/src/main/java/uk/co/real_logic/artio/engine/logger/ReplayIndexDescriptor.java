/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import org.agrona.BitUtil;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;
import uk.co.real_logic.artio.storage.messages.ReplayIndexRecordDecoder;

import java.io.File;
import java.util.Objects;

public final class ReplayIndexDescriptor
{
    private static final int BEGIN_CHANGE_OFFSET = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final int END_CHANGE_OFFSET = BEGIN_CHANGE_OFFSET + BitUtil.SIZE_OF_LONG;

    public static final int INITIAL_RECORD_OFFSET = END_CHANGE_OFFSET + BitUtil.SIZE_OF_LONG;

    static final int RECORD_LENGTH = 32;
    static
    {
        // Safety check against making the ReplayIndexRecord big without modifying this
        if (RECORD_LENGTH < ReplayIndexRecordDecoder.BLOCK_LENGTH) // lgtm [java/constant-comparison]
        {
            throw new IllegalStateException("Invalid record length");
        }
    }

    static File replayIndexFile(final String logFileDir, final long fixSessionId, final int streamId)
    {
        return new File(String.format(logFileDir + File.separator + "replay-index-%d-%d", fixSessionId, streamId));
    }

    static LongHashSet listReplayIndexSessionIds(final File logFileDir, final int streamId)
    {
        final String prefix = "replay-index-";
        final String suffix = "-" + streamId;
        final LongHashSet sessionIds = new LongHashSet();
        for (final File file : Objects.requireNonNull(logFileDir.listFiles()))
        {
            final String fileName = file.getName();
            if (fileName.startsWith(prefix))
            {
                if (fileName.endsWith(suffix))
                {
                    final int suffixIndex = fileName.length() - suffix.length();
                    final String sessionIdString = fileName.substring(prefix.length(), suffixIndex);
                    final long sessionId = Long.parseLong(sessionIdString);
                    sessionIds.add(sessionId);
                }
            }
        }
        return sessionIds;
    }

    public static UnsafeBuffer replayPositionBuffer(final String logFileDir, final int streamId, final int bufferSize)
    {
        final String pathname = replayPositionPath(logFileDir, streamId);
        return new UnsafeBuffer(LoggerUtil.map(new File(pathname), bufferSize));
    }

    static String replayPositionPath(final String logFileDir, final int streamId)
    {
        return logFileDir + File.separator + "replay-positions-" + streamId;
    }

    static void endChangeOrdered(final AtomicBuffer buffer, final long changePosition)
    {
        buffer.putLongOrdered(END_CHANGE_OFFSET, changePosition);
    }

    static long endChangeVolatile(final AtomicBuffer buffer)
    {
        return buffer.getLongVolatile(END_CHANGE_OFFSET);
    }

    static void beginChangeOrdered(final AtomicBuffer buffer, final long changePosition)
    {
        buffer.putLongOrdered(BEGIN_CHANGE_OFFSET, changePosition);
    }

    static long beginChangeVolatile(final AtomicBuffer buffer)
    {
        return buffer.getLongVolatile(BEGIN_CHANGE_OFFSET);
    }

    static long beginChange(final AtomicBuffer buffer)
    {
        return buffer.getLong(BEGIN_CHANGE_OFFSET);
    }

    static int recordCapacity(final int indexFileSize)
    {
        return indexFileSize - INITIAL_RECORD_OFFSET;
    }

    static int offset(final long changePosition, final int capacity)
    {
        return INITIAL_RECORD_OFFSET + ((int)changePosition & (capacity - 1));
    }

    static void checkIndexFileSize(final int indexFileSize)
    {
        final int recordCapacity = recordCapacity(indexFileSize);
        if (!BitUtil.isPowerOfTwo(recordCapacity))
        {
            throw new IllegalStateException(
                "IndexFileSize must be a positive power of 2 + INITIAL_RECORD_OFFSET: indexFileSize=" + indexFileSize);
        }

        if ((recordCapacity % RECORD_LENGTH) != 0)
        {
            throw new IllegalStateException(
                "IndexFileSize must be a multiple of RECORD_LENGTH + INITIAL_RECORD_OFFSET: indexFileSize=" +
                indexFileSize);
        }
    }
}
