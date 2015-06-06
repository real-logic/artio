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

import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.logbuffer.TermReader;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.messages.ArchiveMetaDataDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import static uk.co.real_logic.fix_gateway.logger.LoggerUtil.newArchiveMetaData;

public class ArchivePrinter
{
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();

    private final ArchiveMetaDataDecoder metaDataDecoder;
    private final LogDirectoryDescriptor directoryDescriptor;
    private final int initialTermId;
    private final ExistingBufferFactory bufferFactory;
    private final int streamId;
    private final PrintStream output;

    public static void main(String[] args)
    {
        final int streamId = Integer.parseInt(args[0]);
        final StaticConfiguration configuration = new StaticConfiguration();
        final String logFileDir = configuration.logFileDir();
        final ArchiveMetaData metaData = newArchiveMetaData(configuration);
        final ArchivePrinter printer = new ArchivePrinter(
            LoggerUtil::mapExistingFile, streamId, logFileDir, metaData, System.out);
        printer.print();
    }

    public ArchivePrinter(
        final ExistingBufferFactory bufferFactory,
        final int streamId,
        final String logFileDir,
        final ArchiveMetaData metaData,
        final PrintStream output)
    {
        this.bufferFactory = bufferFactory;
        this.streamId = streamId;
        this.output = output;

        metaDataDecoder = metaData.read(streamId);
        directoryDescriptor = new LogDirectoryDescriptor(logFileDir);
        initialTermId = metaDataDecoder.initialTermId();
    }

    public void print()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(0, 0);
        final TermReader reader = new TermReader(initialTermId, termBuffer);

        for (int termId = initialTermId; true; termId++)
        {
            final File logFile = directoryDescriptor.logFile(streamId, termId);
            if (logFile.exists() && logFile.isFile() && logFile.canRead())
            {
                final ByteBuffer byteBuffer = bufferFactory.map(logFile);
                termBuffer.wrap(byteBuffer);
                reader.read(0, this::printData, Integer.MAX_VALUE);
            }
            else
            {
                break;
            }
        }
    }

    private void printData(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageFrame.wrap(buffer, offset, metaDataDecoder.sbeBlockLength(), metaDataDecoder.sbeSchemaVersion());
        // Technically US-ASCII, but that's a subset of UTF-8
        output.println(messageFrame.body());
    }
}
