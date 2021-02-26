/*
 * Copyright 2021 Monotonic Ltd.
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
package uk.co.real_logic.artio.system_tests;

import b3.entrypoint.fixp.sbe.MessageHeaderDecoder;
import b3.entrypoint.fixp.sbe.MessageHeaderEncoder;
import b3.entrypoint.fixp.sbe.NegotiateEncoder;
import org.agrona.CloseHelper;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.SystemEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointOffsets;
import uk.co.real_logic.sbe.json.JsonPrinter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.LogTag.FIX_TEST;
import static uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointProxy.BINARY_ENTRYPOINT_HEADER_LENGTH;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.*;

public final class BinaryEntrypointClient implements AutoCloseable
{
    private static final int OFFSET = 0;

    public static final int BUFFER_SIZE = 8 * 1024;
    public static final int SESSION_ID = 123;
    public static final int FIRM_ID = 456;
    public static final String SENDER_LOCATION = "LOCATION_1";

    private final JsonPrinter jsonPrinter = new JsonPrinter(BinaryEntryPointOffsets.loadSbeIr());

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final UnsafeBuffer unsafeWriteBuffer = new UnsafeBuffer(writeBuffer);

    private final ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final UnsafeBuffer asciiReadBuffer = new UnsafeBuffer(readBuffer);

    private final EpochNanoClock epochNanoClock = new SystemEpochNanoClock();

    private final SocketChannel socket;
    private final TestSystem testSystem;

    public BinaryEntrypointClient(final int port, final TestSystem testSystem) throws IOException
    {
        socket = SocketChannel.open(new InetSocketAddress("localhost", port));
        this.testSystem = testSystem;
    }

    private void write()
    {
        final int messageSize = readSofhMessageSize(unsafeWriteBuffer, 0);
        writeBuffer.position(0).limit(messageSize);

        testSystem.awaitBlocking(() ->
        {
            try
            {
                print(unsafeWriteBuffer, "< ");

                final int written = socket.write(writeBuffer);
                assertEquals(messageSize, written);
            }
            catch (final IOException e)
            {
                e.printStackTrace();
            }
            finally
            {
                writeBuffer.clear();
            }
        });
    }

    private void wrap(final MessageEncoderFlyweight messageEncoder, final int length)
    {
        final int messageSize = BINARY_ENTRYPOINT_HEADER_LENGTH + length;
        writeBinaryEntryPointSofh(unsafeWriteBuffer, 0, messageSize);
        DebugLogger.log(FIX_TEST, "wrap messageSize=", String.valueOf(messageSize));

        headerEncoder
            .wrap(unsafeWriteBuffer, SOFH_LENGTH)
            .blockLength(messageEncoder.sbeBlockLength())
            .templateId(messageEncoder.sbeTemplateId())
            .schemaId(messageEncoder.sbeSchemaId())
            .version(messageEncoder.sbeSchemaVersion());

        messageEncoder.wrap(unsafeWriteBuffer, BINARY_ENTRYPOINT_HEADER_LENGTH);
    }

    private void print(final UnsafeBuffer unsafeReadBuffer, final String prefixString)
    {
        if (DebugLogger.isEnabled(FIX_TEST))
        {
            final StringBuilder sb = new StringBuilder();
            jsonPrinter.print(sb, unsafeReadBuffer, SOFH_LENGTH);
            DebugLogger.log(FIX_TEST, prefixString, sb.toString());
        }
    }

    public void close()
    {
        CloseHelper.close(socket);
    }

    public void writeNegotiate()
    {
        final NegotiateEncoder negotiate = new NegotiateEncoder();
        wrap(negotiate, NegotiateEncoder.BLOCK_LENGTH);

        negotiate
            .sessionID(SESSION_ID)
            .sessionVerID(1)
            .timestamp().time(epochNanoClock.nanoTime());
        negotiate
            .enteringFirm(FIRM_ID)
            .onbehalfFirm(NegotiateEncoder.onbehalfFirmNullValue())
            .senderLocation(SENDER_LOCATION);

        write();
    }

    public void readNegotiateResponse()
    {

    }

    public void writeEstablish()
    {

    }

    public void readEstablishAck()
    {

    }
}
