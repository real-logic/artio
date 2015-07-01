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
package uk.co.real_logic.fix_gateway.engine.logger;

import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.fix_gateway.StaticConfiguration;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public final class LoggerUtil
{
    public static ByteBuffer map(final File file, final int size)
    {
        if (file.exists())
        {
            return IoUtil.mapExistingFile(file, file.getName());
        }
        else
        {
            return IoUtil.mapNewFile(file, size);
        }
    }

    public static MappedByteBuffer mapExistingFile(final File file)
    {
        return IoUtil.mapExistingFile(file, file.getName());
    }

    public static ArchiveMetaData newArchiveMetaData(final StaticConfiguration configuration)
    {
        final LogDirectoryDescriptor directoryDescriptor = new LogDirectoryDescriptor(configuration.logFileDir());
        return new ArchiveMetaData(directoryDescriptor, LoggerUtil::mapExistingFile, IoUtil::mapNewFile);
    }
}
