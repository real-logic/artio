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

import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.ArchiveMetaDataDecoder;
import uk.co.real_logic.fix_gateway.messages.ArchiveMetaDataEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.function.Function;

import static uk.co.real_logic.fix_gateway.logger.LogDirectoryDescriptor.metaDatalogFile;

public class ArchiveMetaData
{
    public static final int META_DATA_FILE_SIZE = 8 + ArchiveMetaDataDecoder.BLOCK_LENGTH;

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final ArchiveMetaDataDecoder decoder = new ArchiveMetaDataDecoder();
    private final ArchiveMetaDataEncoder encoder = new ArchiveMetaDataEncoder();
    private final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(0, encoder.sbeBlockLength());
    private final Function<File, ByteBuffer> existingBufferFactory;
    private final Function<File, ByteBuffer> newBufferFactory;

    public ArchiveMetaData(
        final Function<File, ByteBuffer> existingBufferFactory, final Function<File, ByteBuffer> newBufferFactory)
    {
        this.existingBufferFactory = existingBufferFactory;
        this.newBufferFactory = newBufferFactory;
    }

    public void write(final int streamId, final int initialTermId, final int termBufferLength)
    {
        ensureBufferNotMapped();
        final File metaDataFile = metaDataFile(streamId);
        if (!metaDataFile.exists())
        {
            metaDataBuffer.wrap(newBufferFactory.apply(metaDataFile));

            headerEncoder
                .wrap(metaDataBuffer, 0, 0)
                .blockLength(encoder.sbeBlockLength())
                .templateId(encoder.sbeTemplateId())
                .schemaId(encoder.sbeSchemaId())
                .version(encoder.sbeSchemaVersion());

            encoder
                .wrap(metaDataBuffer, headerEncoder.size())
                .initialTermId(initialTermId)
                .termBufferLength(termBufferLength);
        }
    }

    public ArchiveMetaDataDecoder read(final int streamId)
    {
        ensureBufferNotMapped();
        metaDataBuffer.wrap(existingBufferFactory.apply(metaDataFile(streamId)));
        headerDecoder.wrap(metaDataBuffer, 0, 0);
        decoder.wrap(metaDataBuffer, headerDecoder.size(), headerDecoder.blockLength(), headerDecoder.version());
        return decoder;
    }

    private File metaDataFile(int streamId)
    {
        return new File(metaDatalogFile(streamId));
    }

    private void ensureBufferNotMapped()
    {
        final ByteBuffer buffer = metaDataBuffer.byteBuffer();
        if (buffer != null && buffer instanceof MappedByteBuffer)
        {
            IoUtil.unmap((MappedByteBuffer) buffer);
        }
    }
}
