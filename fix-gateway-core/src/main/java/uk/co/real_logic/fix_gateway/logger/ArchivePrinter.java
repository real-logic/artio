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
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.messages.ArchiveMetaDataDecoder;
import uk.co.real_logic.fix_gateway.replication.DataSubscriber;
import uk.co.real_logic.fix_gateway.session.SessionHandler;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.fix_gateway.logger.LoggerUtil.newArchiveMetaData;

public class ArchivePrinter implements SessionHandler
{
    public static final int UNKNOWN = -1;

    private final DataSubscriber subscriber = new DataSubscriber(this);
    private final AsciiFlyweight ascii = new AsciiFlyweight();

    private final LogDirectoryDescriptor directoryDescriptor;
    private final ExistingBufferFactory bufferFactory;
    private final int streamId;
    private final PrintStream output;

    public static void main(String[] args)
    {
        if (args.length < 1)
        {
            System.err.println("Usage: ArchivePrinter <streamId>");
            System.exit(-1);
        }

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

        final ArchiveMetaDataDecoder metaDataDecoder = metaData.read(streamId);
        directoryDescriptor = new LogDirectoryDescriptor(logFileDir);
    }

    public void print()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(0, 0);

        for (final File logFile : directoryDescriptor.listLogFiles(streamId))
        {
            final ByteBuffer byteBuffer = bufferFactory.map(logFile);
            if (byteBuffer.capacity() > 0)
            {
                termBuffer.wrap(byteBuffer);

                for (int offset = HEADER_LENGTH; offset > 0 && offset < termBuffer.capacity(); offset += HEADER_LENGTH)
                {
                    if (termBuffer.getByte(offset) == 0)
                    {
                        break;
                    }

                    offset = subscriber.readFragment(termBuffer, offset);
                }
            }
        }
    }

    public void onMessage(final DirectBuffer buffer,
                          final int offset,
                          final int length,
                          final long connectionId,
                          final long sessionId,
                          final int messageType)
    {
        ascii.wrap(buffer);
        output.println(ascii.getAscii(offset, length));
    }

    public void onDisconnect(final long connectionId)
    {
        System.out.println("disconnect");
    }
}
