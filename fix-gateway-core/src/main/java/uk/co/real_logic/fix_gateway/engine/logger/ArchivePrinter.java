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

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.TermReader;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;
import uk.co.real_logic.fix_gateway.sbe_util.MessageDumper;
import uk.co.real_logic.fix_gateway.sbe_util.MessageSchemaIr;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;

/**
 * Eg: -Dlogging.dir=/home/richard/monotonic/Fix-Engine/fix-gateway-system-tests/client-logs \
 * ArchivePrinter 'UDP-00000000-0-7f000001-10048' 0
 */
public class ArchivePrinter implements FragmentHandler
{
    private static final int CHANNEL_ARG = 0;
    private static final int ID_ARG = 1;

    private final AsciiBuffer ascii = new MutableAsciiBuffer();

    private final LogDirectoryDescriptor directoryDescriptor;
    private final ExistingBufferFactory bufferFactory;
    private final StreamIdentifier streamId;
    private final PrintStream output;
    private final MessageDumper dumper = new MessageDumper(MessageSchemaIr.SCHEMA_BUFFER);
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();

    public static void main(final String[] args)
    {
        if (args.length < 2)
        {
            System.err.println("Usage: ArchivePrinter <channel> <streamId>");
            System.exit(-1);
        }

        final StreamIdentifier streamId = new StreamIdentifier(args[CHANNEL_ARG], Integer.parseInt(args[ID_ARG]));
        final EngineConfiguration configuration = new EngineConfiguration();
        final String logFileDir = configuration.logFileDir();
        final ArchivePrinter printer = new ArchivePrinter(LoggerUtil::mapExistingFile, streamId, logFileDir, System.out);
        printer.print();
    }

    public ArchivePrinter(
        final ExistingBufferFactory bufferFactory,
        final StreamIdentifier streamId,
        final String logFileDir,
        final PrintStream output)
    {
        this.bufferFactory = bufferFactory;
        this.streamId = streamId;
        this.output = output;

        directoryDescriptor = new LogDirectoryDescriptor(logFileDir);
    }

    public void print()
    {
        final UnsafeBuffer termBuffer = new UnsafeBuffer(0, 0);
        for (final File logFile : directoryDescriptor.listLogFiles(streamId))
        {
            output.printf("Printing %s\n", logFile);
            final ByteBuffer byteBuffer = bufferFactory.map(logFile);
            if (byteBuffer.capacity() > 0)
            {
                termBuffer.wrap(byteBuffer);
                final int initialTermId = LogBufferDescriptor.initialTermId(termBuffer);
                final Header header = new Header(initialTermId, termBuffer.capacity());
                final long messagesRead = TermReader.read(
                    termBuffer,
                    0,
                    this,
                    Integer.MAX_VALUE,
                    header,
                    Throwable::printStackTrace);
                output.printf("Read %d messages\n", messagesRead);
            }
        }
    }

    public void onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        headerDecoder.wrap(buffer, offset);

        final String result = dumper.toString(
            headerDecoder.templateId(),
            headerDecoder.version(),
            headerDecoder.blockLength(),
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH
        );

        output.println(result);
    }
}
