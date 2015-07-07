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
package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.CountersManager;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.nio.MappedByteBuffer;

/**
 *
 * A single file of the labelsBuffer, followed by the countersBuffer.
 *
 */
public final class CountersFile implements AutoCloseable
{
    private final MappedByteBuffer mappedByteBuffer;
    private final AtomicBuffer labelsBuffer;
    private final AtomicBuffer countersBuffer;

    public CountersFile(final boolean newFile, final StaticConfiguration configuration)
    {
        final File file = new File(configuration.counterBuffersFile());
        final int length;
        if (newFile)
        {
            IoUtil.deleteIfExists(file);

            length = configuration.counterBuffersLength();
            mappedByteBuffer = IoUtil.mapNewFile(file, length * 2);
        }
        else
        {
            if (!file.exists() || !file.canRead() || !file.isFile())
            {
                throw new IllegalStateException("Unable to read from file: " + file);
            }

            mappedByteBuffer = IoUtil.mapExistingFile(file, "counters file");
            length = mappedByteBuffer.capacity() / 2;
        }

        final AtomicBuffer mappedFile = new UnsafeBuffer(mappedByteBuffer);
        labelsBuffer = new UnsafeBuffer(mappedFile, 0, length);
        countersBuffer = new UnsafeBuffer(mappedFile, length, length);
    }

    public CountersManager createCountersManager()
    {
        return new CountersManager(labelsBuffer, countersBuffer);
    }

    public AtomicBuffer countersBuffer()
    {
        return countersBuffer;
    }

    public void close()
    {
        IoUtil.unmap(mappedByteBuffer);
    }
}
