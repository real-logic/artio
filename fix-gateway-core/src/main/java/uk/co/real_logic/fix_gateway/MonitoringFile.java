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
 * A memory mapped file that stores monitoring data which can be accessed by a monitoring
 * daemon/process.
 *
 * This contains buffers in order for:
 *
 * <ol>
 *     <li>The Labels Buffer</li>
 *     <li>The Counters Buffer</li>
 *     <li>The Error Buffer</li>
 * </ol>
 */
public final class MonitoringFile implements AutoCloseable
{
    private static final int NUMBER_OF_BUFFERS = 3;

    private final MappedByteBuffer mappedByteBuffer;
    private final AtomicBuffer labelsBuffer;
    private final AtomicBuffer countersBuffer;
    private final AtomicBuffer errorBuffer;

    public MonitoringFile(final boolean newFile, final CommonConfiguration configuration)
    {
        final File file = new File(configuration.monitoringFile()).getAbsoluteFile();
        final int length;
        if (newFile)
        {
            IoUtil.deleteIfExists(file);

            length = configuration.monitoringBuffersLength();
            mappedByteBuffer = IoUtil.mapNewFile(file, length * NUMBER_OF_BUFFERS);
        }
        else
        {
            if (!file.exists() || !file.canRead() || !file.isFile())
            {
                throw new IllegalStateException("Unable to read from file: " + file);
            }

            mappedByteBuffer = IoUtil.mapExistingFile(file, "counters file");
            length = mappedByteBuffer.capacity() / NUMBER_OF_BUFFERS;
        }

        final AtomicBuffer mappedFile = new UnsafeBuffer(mappedByteBuffer);
        labelsBuffer = new UnsafeBuffer(mappedFile, 0, length);
        countersBuffer = new UnsafeBuffer(mappedFile, length, length);
        errorBuffer = new UnsafeBuffer(mappedFile, 2 * length, length);
    }

    public CountersManager createCountersManager()
    {
        return new CountersManager(labelsBuffer, countersBuffer);
    }

    public AtomicBuffer countersBuffer()
    {
        return countersBuffer;
    }

    public AtomicBuffer errorBuffer()
    {
        return errorBuffer;
    }

    public void close()
    {
        IoUtil.unmap(mappedByteBuffer);
    }
}
