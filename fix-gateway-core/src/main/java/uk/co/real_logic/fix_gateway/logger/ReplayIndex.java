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
package uk.co.real_logic.fix_gateway.logger;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.IoUtil;

import java.nio.MappedByteBuffer;

public class ReplayIndex implements Index
{
    private final Index index;

    private MappedByteBuffer currentFileBuffer;

    public ReplayIndex(final Index index)
    {
        this.index = index;
    }

    public void newIndexFile(final int id)
    {
        close();
        currentFileBuffer = null;
        //IoUtil.mapNewFile(LogDirectoryDescriptor.indexFile(index.getName(), id), INDEX_FILE_SIZE);
    }

    public void close()
    {
        if (currentFileBuffer != null)
        {
            IoUtil.unmap(currentFileBuffer);
        }
    }

    public void indexRecord(final DirectBuffer buffer, final int offset, final int length)
    {
        final long value = 0L;
        //index.extractKey(buffer, offset, length);
        currentFileBuffer.putLong(value);
    }
}
