/*
 * Copyright 2015-2023 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public final class LoggerUtil
{
    public static ByteBuffer map(final File file, final int size)
    {
        if (file.exists())
        {
            final long fileLength = file.length();
            if (fileLength < size)
            {
                throw new IllegalArgumentException("Invalid file size: " +
                    file.getAbsolutePath() + " size=" + size + ", fileLength=" + fileLength);
            }

            return IoUtil.mapExistingFile(file, file.getName());
        }
        else
        {
            return mapNewFile(file, size);
        }
    }

    public static MappedByteBuffer mapNewFile(final File file, final int size)
    {
        final File parentDir = file.getParentFile();
        IoUtil.ensureDirectoryExists(parentDir, parentDir.getAbsolutePath());

        return IoUtil.mapNewFile(file, size);
    }

    public static MappedByteBuffer mapExistingFile(final File file)
    {
        return IoUtil.mapExistingFile(file, file.getName());
    }

    // Returns true if the buffer has been initialised this time round, false if it was already initialised.
    public static boolean initialiseBuffer(
        final AtomicBuffer buffer,
        final MessageHeaderEncoder headerEncoder,
        final MessageHeaderDecoder headerDecoder,
        final int sbeSchemaId,
        final int sbeTemplateId,
        final int actingVersion,
        final int actingBlockLength,
        final ErrorHandler errorHandler)
    {
        headerDecoder.wrap(buffer, 0);
        if (headerDecoder.blockLength() == 0)
        {
            writeHeader(buffer, headerEncoder, sbeSchemaId, sbeTemplateId, actingVersion, actingBlockLength);

            return true;
        }
        else
        {
            if (!validateBuffer(
                buffer,
                headerDecoder,
                sbeSchemaId,
                errorHandler))
            {
                writeHeader(
                    buffer, headerEncoder, sbeSchemaId, sbeTemplateId, actingVersion, actingBlockLength);

                final int offset = headerEncoder.encodedLength();
                final int length = buffer.capacity() - offset;
                buffer.setMemory(
                    offset,
                    length,
                    (byte)0);
            }

            return false;
        }
    }

    private static void writeHeader(
        final AtomicBuffer buffer,
        final MessageHeaderEncoder headerEncoder,
        final int sbeSchemaId,
        final int sbeTemplateId,
        final int actingVersion,
        final int actingBlockLength)
    {
        headerEncoder
            .wrap(buffer, 0)
            .blockLength(actingBlockLength)
            .templateId(sbeTemplateId)
            .schemaId(sbeSchemaId)
            .version(actingVersion);
    }

    // Returns false if not valid
    static boolean validateBuffer(
        final AtomicBuffer buffer,
        final MessageHeaderDecoder headerDecoder,
        final int sbeSchemaId,
        final ErrorHandler errorHandler)
    {
        headerDecoder.wrap(buffer, 0);
        return validateField(sbeSchemaId, headerDecoder.schemaId(), "Schema Id", errorHandler);
    }

    private static boolean validateField(
        final int expected,
        final int read,
        final String name,
        final ErrorHandler errorHandler)
    {
        if (read != expected)
        {
            final IllegalStateException exception = new IllegalStateException(
                String.format("Wrong %s: expected %d and got %d", name, expected, read));
            errorHandler.onError(exception);

            return false;
        }

        return true;
    }
}
