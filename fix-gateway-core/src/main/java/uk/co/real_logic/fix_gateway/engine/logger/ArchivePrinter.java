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

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;
import uk.co.real_logic.fix_gateway.sbe_util.MessageDumper;
import uk.co.real_logic.fix_gateway.sbe_util.MessageSchemaIr;

import java.io.PrintStream;

/**
 * Eg: -Dlogging.dir=/home/richard/monotonic/Fix-Engine/fix-gateway-system-tests/client-logs \
 * ArchivePrinter 'UDP-00000000-0-7f000001-10048' 0
 */
public class ArchivePrinter implements FragmentHandler
{
    private static final int CHANNEL_ARG = 0;
    private static final int ID_ARG = 1;

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
        final ArchiveScanner scanner = new ArchiveScanner(logFileDir);
        scanner.forEachFragment(streamId, new ArchivePrinter(System.out), Throwable::printStackTrace);
    }

    public ArchivePrinter(
        final PrintStream output)
    {
        this.output = output;
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
