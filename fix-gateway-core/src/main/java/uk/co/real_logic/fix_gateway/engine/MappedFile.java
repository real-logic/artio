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
package uk.co.real_logic.fix_gateway.engine;

import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.CommonConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;

public class MappedFile implements AutoCloseable
{
    private final FileChannel fileChannel;
    private final AtomicBuffer buffer;

    public static MappedFile map(final String bufferPath, final int size)
    {
        final FileChannel fileChannel;
        final File bufferFile = new File(bufferPath);
        try
        {
            if (bufferFile.exists())
            {
                // NB: closing RAF or FileChannel closes them both
                fileChannel = new RandomAccessFile(bufferFile, "rw").getChannel();
            }
            else
            {
                fileChannel = IoUtil.createEmptyFile(bufferFile, (long) size);
            }

            final MappedByteBuffer mappedBuffer = fileChannel.map(READ_WRITE, 0, fileChannel.size());
            return new MappedFile(fileChannel, new UnsafeBuffer(mappedBuffer));
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
            return null;
        }
    }

    public MappedFile(final FileChannel fileChannel, final AtomicBuffer buffer)
    {
        this.fileChannel = fileChannel;
        this.buffer = buffer;
    }

    public AtomicBuffer buffer()
    {
        return buffer;
    }

    public void force()
    {
        force(fileChannel);
    }

    public void transferTo(final File backupLocation)
    {
        try (final FileChannel backupChannel = FileChannel.open(backupLocation.toPath(), CREATE_NEW))
        {
            fileChannel.transferTo(0L, fileChannel.size(), backupChannel);
            force(backupChannel);
        }
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    public void close()
    {
        IoUtil.unmap(buffer.byteBuffer());
        CloseHelper.close(fileChannel);
    }

    private void force(final FileChannel fileChannel)
    {
        if (CommonConfiguration.FORCE_WRITES)
        {
            try
            {
                fileChannel.force(true);
            }
            catch (IOException e)
            {
                LangUtil.rethrowUnchecked(e);
            }
        }
    }
}
