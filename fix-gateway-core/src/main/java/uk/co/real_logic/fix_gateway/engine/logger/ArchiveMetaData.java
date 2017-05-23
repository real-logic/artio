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

import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.storage.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.storage.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;
import uk.co.real_logic.fix_gateway.storage.messages.ArchiveMetaDataDecoder;
import uk.co.real_logic.fix_gateway.storage.messages.ArchiveMetaDataEncoder;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder.ENCODED_LENGTH;

public class ArchiveMetaData implements AutoCloseable
{
    private static final int META_DATA_FILE_SIZE = 8 + ArchiveMetaDataDecoder.BLOCK_LENGTH;
    private static final int MINIMUM_BUFFER_SIZE = ENCODED_LENGTH + ArchiveMetaDataDecoder.BLOCK_LENGTH;

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final ArchiveMetaDataDecoder decoder = new ArchiveMetaDataDecoder();
    private final ArchiveMetaDataEncoder metaDataEncoder = new ArchiveMetaDataEncoder();
    private final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(0, metaDataEncoder.sbeBlockLength());
    private final ExistingBufferFactory existingBufferFactory;
    private final BufferFactory newBufferFactory;
    private final LogDirectoryDescriptor directoryDescriptor;

    public ArchiveMetaData(final LogDirectoryDescriptor directoryDescriptor)
    {
        this(directoryDescriptor, LoggerUtil::mapExistingFile, LoggerUtil::map);
    }

    ArchiveMetaData(
        final LogDirectoryDescriptor directoryDescriptor,
        final ExistingBufferFactory existingBufferFactory,
        final BufferFactory newBufferFactory)
    {
        this.directoryDescriptor = directoryDescriptor;
        this.existingBufferFactory = existingBufferFactory;
        this.newBufferFactory = newBufferFactory;
    }

    public void write(
        final StreamIdentifier streamId,
        final int sessionId,
        final int initialTermId,
        final int termBufferLength)
    {
        ensureBufferNotMapped();
        final File metaDataFile = directoryDescriptor.metaDataLogFile(streamId, sessionId);
        if (!metaDataFile.exists())
        {
            metaDataBuffer.wrap(newBufferFactory.map(metaDataFile, META_DATA_FILE_SIZE));

            metaDataEncoder
                .wrapAndApplyHeader(metaDataBuffer, 0, headerEncoder)
                .initialTermId(initialTermId)
                .termBufferLength(termBufferLength);
        }
    }

    public ArchiveMetaDataDecoder read(final StreamIdentifier streamId, final int sessionId)
    {
        ensureBufferNotMapped();
        final File file = directoryDescriptor.metaDataLogFile(streamId, sessionId);
        if (!file.exists())
        {
            return null;
        }

        final ByteBuffer buffer = existingBufferFactory.map(file);
        if (buffer.capacity() < MINIMUM_BUFFER_SIZE)
        {
            return null;
        }

        metaDataBuffer.wrap(buffer);

        int offset = 0;
        headerDecoder.wrap(metaDataBuffer, offset);

        offset += ENCODED_LENGTH;

        decoder.wrap(metaDataBuffer, offset, headerDecoder.blockLength(), headerDecoder.version());

        return decoder;
    }

    private void ensureBufferNotMapped()
    {
        final ByteBuffer buffer = metaDataBuffer.byteBuffer();
        if (buffer != null && buffer instanceof MappedByteBuffer)
        {
            IoUtil.unmap((MappedByteBuffer)buffer);
        }
    }

    public LogDirectoryDescriptor directoryDescriptor()
    {
        return directoryDescriptor;
    }

    public void close()
    {
        ensureBufferNotMapped();
    }
}
