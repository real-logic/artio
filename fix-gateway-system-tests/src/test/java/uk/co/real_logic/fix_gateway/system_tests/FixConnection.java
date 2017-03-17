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
package uk.co.real_logic.fix_gateway.system_tests;

import org.agrona.LangUtil;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.builder.*;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.fix_gateway.LogTag.FIX_TEST;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.ACCEPTOR_ID;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.INITIATOR_ID;

class FixConnection implements AutoCloseable
{
    private static final int BUFFER_SIZE = 8 * 1024;
    private static final int OFFSET = 0;

    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final MutableAsciiBuffer writeAsciiBuffer = new MutableAsciiBuffer(writeBuffer);

    private final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
    private final LogonEncoder logon = new LogonEncoder();
    private final LogoutEncoder logout = new LogoutEncoder();
    private final HeartbeatEncoder heartbeatEncoder = new HeartbeatEncoder();

    private final SocketChannel socket;

    private int msgSeqNum = 1;

    FixConnection(final int port) throws IOException
    {
        socket = SocketChannel.open(new InetSocketAddress("localhost", port));
    }

    void logon(final long timestamp)
    {
        setupHeader(logon.header(), timestamp);

        logon.resetSeqNumFlag(true)
             .encryptMethod(0)
             .heartBtInt(30)
             .maxMessageSize(9999);

        send(logon);
    }

    void heartbeat(final long timestamp)
    {
        setupHeader(heartbeatEncoder.header(), timestamp);

        send(heartbeatEncoder);
    }

    void logout()
    {
        setupHeader(logout.header(), System.currentTimeMillis());

        send(logout);
    }

    private void setupHeader(final HeaderEncoder header, final long timestamp)
    {
        final int timestampLength = timestampEncoder.encode(timestamp);

        header
            .senderCompID(INITIATOR_ID)
            .targetCompID(ACCEPTOR_ID)
            .msgSeqNum(msgSeqNum++)
            .sendingTime(timestampEncoder.buffer(), timestampLength);
    }

    void readMessage(final Decoder decoder)
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer(buffer);

        try
        {
            final int read = socket.read(buffer);
            DebugLogger.log(FIX_TEST, "< [" + asciiBuffer.getAscii(OFFSET, read) + "]");
            decoder.decode(asciiBuffer, OFFSET, read);
        }
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    private void send(final Encoder encoder)
    {
        try
        {
            final long result = encoder.encode(writeAsciiBuffer, OFFSET);
            final int offset = Encoder.offset(result);
            final int length = Encoder.length(result);
            encoder.reset();
            writeBuffer.position(offset).limit(offset + length);
            final int written = socket.write(writeBuffer);
            assertEquals(length, written);
            DebugLogger.log(FIX_TEST, "> [" + writeAsciiBuffer.getAscii(OFFSET, length) + "]");
            writeBuffer.clear();
        }
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    public void close()
    {
        try
        {
            socket.close();
        }
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }
}
