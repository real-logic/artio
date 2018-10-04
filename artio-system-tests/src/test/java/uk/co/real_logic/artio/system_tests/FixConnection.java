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
package uk.co.real_logic.artio.system_tests;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.builder.*;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.LogTag.FIX_TEST;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

final class FixConnection implements AutoCloseable
{
    private static final int BUFFER_SIZE = 8 * 1024;
    private static final int OFFSET = 0;

    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final MutableAsciiBuffer writeAsciiBuffer = new MutableAsciiBuffer(writeBuffer);

    private final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
    private final LogonEncoder logon = new LogonEncoder();
    private final LogoutEncoder logout = new LogoutEncoder();

    private final SocketChannel socket;
    private final String senderCompID;
    private final String targetCompID;

    private int msgSeqNum = 1;

    static FixConnection initiate(final int port) throws IOException
    {
        return new FixConnection(
            SocketChannel.open(new InetSocketAddress("localhost", port)),
            INITIATOR_ID,
            ACCEPTOR_ID);
    }

    static FixConnection accept(final int port, final Runnable connectOperation) throws IOException
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
                ADMIN_IDLE_STRATEGY.idle();
            }
            ADMIN_IDLE_STRATEGY.reset();

            return new FixConnection(
                socket,
                ACCEPTOR_ID,
                INITIATOR_ID);
        }
    }

    private FixConnection(final SocketChannel socket, final String senderCompID, final String targetCompID)
    {
        this.socket = socket;
        this.senderCompID = senderCompID;
        this.targetCompID = targetCompID;
    }

    void logon(final boolean resetSeqNumFlag)
    {
        final long timestamp = System.currentTimeMillis();
        setupHeader(logon.header(), timestamp);

        logon
            .resetSeqNumFlag(resetSeqNumFlag)
            .encryptMethod(0)
            .heartBtInt(30)
            .maxMessageSize(9999);

        send(logon);
    }

    public FixConnection msgSeqNum(final int msgSeqNum)
    {
        this.msgSeqNum = msgSeqNum;
        return this;
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
            .senderCompID(senderCompID)
            .targetCompID(targetCompID)
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
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
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
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public LogonDecoder readLogonReply()
    {
        final LogonDecoder logon = new LogonDecoder();
        readMessage(logon);

        assertTrue(logon.validate());

        return logon;
    }

    public void close()
    {
        CloseHelper.close(socket);
    }
}
