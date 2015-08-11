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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.EngineConfiguration;
import uk.co.real_logic.fix_gateway.library.session.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.ConnectionType;
import uk.co.real_logic.fix_gateway.replication.DataSubscriber;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

/**
 * Eg: -Dlogging.dir=/home/richard/monotonic/Fix-Engine/fix-gateway-system-tests/client-logs ArchivePrinter 0
 */
public class ArchivePrinter implements SessionHandler
{

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
        final EngineConfiguration configuration = new EngineConfiguration();
        final String logFileDir = configuration.logFileDir();
        final ArchivePrinter printer = new ArchivePrinter(
            LoggerUtil::mapExistingFile, streamId, logFileDir, System.out);
        printer.print();
    }

    public ArchivePrinter(
        final ExistingBufferFactory bufferFactory,
        final int streamId,
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
            // System.out.printf("Printing %s\n", logFile);
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

                   offset = subscriber.readFragment(termBuffer, offset, streamId);
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
        output.printf("%d Disconnected\n", connectionId);
    }

    public void onLogon(final long connectionId, final long sessionId)
    {
        output.printf("connection %d has logged in as session %d\n", connectionId, sessionId);
    }

    public void onConnect(final int sessionId,
                          final long connectionId,
                          final ConnectionType type,
                          final DirectBuffer buffer,
                          final int addressOffset,
                          final int addressLength)
    {
        final String address = buffer.getStringUtf8(addressOffset, addressLength);
        output.printf("Connected to %s as connection %d\n", address, connectionId);
    }

    public void onInitiateConnection(final int libraryId,
                                     final int port,
                                     final String host,
                                     final String senderCompId,
                                     final String targetCompId)
    {
        output.printf("Initiate Connection to %s:%d as %s to %s", host, port, senderCompId, targetCompId);
    }
}
