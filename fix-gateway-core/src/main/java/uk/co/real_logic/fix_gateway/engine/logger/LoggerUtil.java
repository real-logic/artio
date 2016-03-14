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
package uk.co.real_logic.fix_gateway.engine.logger;

import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;

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

    public static ArchiveMetaData newArchiveMetaData(final String logFileDir)
    {
        final LogDirectoryDescriptor directoryDescriptor = new LogDirectoryDescriptor(logFileDir);
        return new ArchiveMetaData(directoryDescriptor, LoggerUtil::mapExistingFile, IoUtil::mapNewFile);
    }

    /**
     * Returns true if the buffer has been initialised this time round, false if it was already initialised.
     */
    public static boolean initialiseBuffer(
        final AtomicBuffer buffer,
        final MessageHeaderEncoder headerEncoder,
        final MessageHeaderDecoder headerDecoder,
        final int sbeSchemaId,
        final int sbeTemplateId,
        final int actingVersion,
        final int actingBlockLength)
    {
        headerDecoder.wrap(buffer, 0);
        if (headerDecoder.blockLength() == 0)
        {
            headerEncoder
                .wrap(buffer, 0)
                .blockLength(actingBlockLength)
                .templateId(sbeTemplateId)
                .schemaId(sbeSchemaId)
                .version(actingVersion);

            return true;
        }
        else
        {
            validateBuffer(
                buffer,
                headerDecoder,
                sbeSchemaId,
                actingVersion,
                actingBlockLength);

            return false;
        }
    }

    public static void validateBuffer(
        final AtomicBuffer buffer,
        final MessageHeaderDecoder headerDecoder,
        final int sbeSchemaId,
        final int actingVersion,
        final int actingBlockLength)
    {
        headerDecoder.wrap(buffer, 0);
        validateField(sbeSchemaId, headerDecoder.schemaId(), "Schema Id");
        validateField(actingVersion, headerDecoder.version(), "Schema Version");
        validateField(actingBlockLength, headerDecoder.blockLength(), "Block Length");
    }

    private static void validateField(final int expected, final int read, final String name)
    {
        if (read != expected)
        {
            throw new IllegalStateException(
                String.format("Wrong %s: expected %d and got %d", name, expected, read));
        }
    }
}
