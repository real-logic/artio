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

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertEquals;

public final class BinaryEntrypointClient implements AutoCloseable
{
    public static final int BUFFER_SIZE = 8 * 1024;
    private static final int OFFSET = 0;

    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final UnsafeBuffer writeAsciiBuffer = new UnsafeBuffer(writeBuffer);

    private final ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final UnsafeBuffer asciiReadBuffer = new UnsafeBuffer(readBuffer);

    private final SocketChannel socket;

    public BinaryEntrypointClient(final int port) throws IOException
    {
        socket = SocketChannel.open(new InetSocketAddress("localhost", port));
    }

    private void send(final int offset, final int length)
    {
        try
        {
            writeBuffer.position(offset).limit(offset + length);
            final int written = socket.write(writeBuffer);
            assertEquals(length, written);
            writeBuffer.clear();
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private int readData() throws IOException
    {
        return socket.read(readBuffer);
    }

    public void close()
    {
        CloseHelper.close(socket);
    }

    public void sendNegotiate()
    {

    }

    public void readNegotiateResponse()
    {

    }

    public void sendEstablish()
    {

    }

    public void readEstablishAck()
    {

    }
}
