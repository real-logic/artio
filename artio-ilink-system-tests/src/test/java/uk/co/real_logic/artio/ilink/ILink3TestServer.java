/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.ilink;

import iLinkBinary.MessageHeaderDecoder;
import iLinkBinary.MessageHeaderEncoder;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageDecoderFlyweight;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.system_tests.TestSystem;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static iLinkBinary.MessageHeaderEncoder.ENCODED_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;

public class ILink3TestServer
{
    private static final int BUFFER_SIZE = 8 * 1024;

    private final SocketChannel socket;
    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final UnsafeBuffer unsafeWriteBuffer = new UnsafeBuffer(writeBuffer);
    private final UnsafeBuffer userWriteBuffer = new UnsafeBuffer(
        unsafeWriteBuffer, ENCODED_LENGTH, BUFFER_SIZE - ENCODED_LENGTH);
    private final ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final UnsafeBuffer unsafeReadBuffer = new UnsafeBuffer(readBuffer);

    private final MessageHeaderDecoder iLinkHeaderDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder iLinkHeaderEncoder = new MessageHeaderEncoder();

    public ILink3TestServer(
        final int port,
        final Runnable connectOperation,
        final TestSystem testSystem) throws IOException
    {
        try (ServerSocketChannel server = ServerSocketChannel
            .open()
            .bind(new InetSocketAddress("localhost", port)))
        {
            server.configureBlocking(false);

            connectOperation.run();

            SocketChannel socket;
            while ((socket = server.accept()) == null)
            {
                testSystem.poll();
                Thread.yield();
            }

            this.socket = socket;
        }

        iLinkHeaderDecoder.wrap(unsafeReadBuffer, 0);
        iLinkHeaderEncoder.wrap(unsafeWriteBuffer, 0);
    }

    public <T extends MessageDecoderFlyweight> T read(final T messageDecoder)
    {
        try
        {
            final int read = socket.read(readBuffer);
            final int encodedLength = iLinkHeaderDecoder.encodedLength();
            final int blockLength = iLinkHeaderDecoder.blockLength();
            messageDecoder.wrap(
                unsafeReadBuffer,
                encodedLength,
                blockLength,
                iLinkHeaderDecoder.version());

            assertEquals(messageDecoder.sbeTemplateId(), iLinkHeaderDecoder.templateId());
            assertThat(read, greaterThanOrEqualTo(encodedLength + blockLength));

            readBuffer.clear();

            return messageDecoder;
        }
        catch (final IOException e)
        {
            LangUtil.rethrowUnchecked(e);
            return null;
        }
    }

    public UnsafeBuffer writeBuffer()
    {
        return userWriteBuffer;
    }

    public void write(final MessageEncoderFlyweight messageEncoder)
    {
        final int length = messageEncoder.offset() + messageEncoder.encodedLength();
        writeBuffer.position(0).limit(length);
        iLinkHeaderEncoder
            .wrap(unsafeWriteBuffer, 0)
            .blockLength(messageEncoder.sbeBlockLength())
            .templateId(messageEncoder.sbeTemplateId())
            .schemaId(messageEncoder.sbeSchemaId())
            .version(messageEncoder.sbeSchemaVersion());

        try
        {
            final int written = socket.write(writeBuffer);
            assertEquals(length, written);
        }
        catch (final IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            writeBuffer.clear();
        }
    }
}
