/*
 * Copyright 2015-2017 Real Logic Ltd.
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
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.ExecType;
import uk.co.real_logic.artio.OrdStatus;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.builder.*;
import uk.co.real_logic.artio.decoder.HeartbeatDecoder;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.decoder.LogoutDecoder;
import uk.co.real_logic.artio.fields.RejectReason;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.*;
import static uk.co.real_logic.artio.LogTag.FIX_TEST;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

final class FixConnection implements AutoCloseable
{
    private static final int BUFFER_SIZE = 8 * 1024;
    private static final int OFFSET = 0;

    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final MutableAsciiBuffer writeAsciiBuffer = new MutableAsciiBuffer(writeBuffer);

    private final UtcTimestampEncoder sendingTimeEncoder = new UtcTimestampEncoder();
    private final UtcTimestampEncoder origSendingTimeEncoder = new UtcTimestampEncoder();
    private final LogonEncoder logon = new LogonEncoder();
    private final LogoutEncoder logout = new LogoutEncoder();
    private final TestRequestEncoder testRequestEncoder = new TestRequestEncoder();

    private final SocketChannel socket;
    private final String senderCompID;
    private final String targetCompID;

    private int msgSeqNum = 1;

    private final ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final MutableAsciiBuffer asciiReadBuffer = new MutableAsciiBuffer(readBuffer);
    private int bytesRemaining = 0;

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

    boolean isConnected()
    {
        return socket.isConnected();
    }

    void logon(final boolean resetSeqNumFlag)
    {
        setupHeader(logon.header(), msgSeqNum++, false);

        logon
            .resetSeqNumFlag(resetSeqNumFlag)
            .encryptMethod(0)
            .heartBtInt(30)
            .maxMessageSize(9999);

        send(logon);
    }

    FixConnection msgSeqNum(final int msgSeqNum)
    {
        this.msgSeqNum = msgSeqNum;
        return this;
    }

    void logout()
    {
        setupHeader(logout.header(), msgSeqNum++, false);

        send(logout);
    }

    void setupHeader(final HeaderEncoder header, final int msgSeqNum, final boolean possDupFlag)
    {
        final long timestamp = System.currentTimeMillis();
        final int timestampLength = sendingTimeEncoder.encode(timestamp);

        header
            .senderCompID(senderCompID)
            .targetCompID(targetCompID)
            .msgSeqNum(msgSeqNum)
            .sendingTime(sendingTimeEncoder.buffer(), timestampLength);

        if (possDupFlag)
        {
            final int origSendingTimeLength = origSendingTimeEncoder.encode(timestamp - 1000);

            header
                .possDupFlag(true)
                .origSendingTime(origSendingTimeEncoder.buffer(), origSendingTimeLength);
        }
    }

    <T extends Decoder> T readMessage(final T decoder)
    {
        try
        {
            final int bytesToParse = bytesRemaining == 0 ? socket.read(readBuffer) : bytesRemaining;
            final String ascii = asciiReadBuffer.getAscii(OFFSET, bytesToParse);

            DebugLogger.log(FIX_TEST,
                "< [" + ascii + "] for attempted: " + decoder.getClass());

            int endOfMessage = ascii.indexOf("8=FIX.4.4", 9);
            if (endOfMessage == -1)
            {
                endOfMessage = bytesToParse;
            }

            decoder.decode(asciiReadBuffer, OFFSET, endOfMessage);

            if (!decoder.validate())
            {
                fail("Failed: " + RejectReason.decode(decoder.rejectReason()) + " for " + decoder.invalidTagId());
            }

            readBuffer.clear();
            if (endOfMessage != -1)
            {
                bytesRemaining = bytesToParse - endOfMessage;
                asciiReadBuffer.putBytes(0, asciiReadBuffer, endOfMessage, bytesRemaining);
            }
            else
            {
                bytesRemaining = 0;
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return decoder;
    }

    int pollData() throws IOException
    {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer(buffer);

        socket.configureBlocking(false);
        final int read = socket.read(buffer);
        socket.configureBlocking(true);
        if (read > 0)
        {
            DebugLogger.log(FIX_TEST, "< [" + asciiBuffer.getAscii(OFFSET, read) + "] for poll");
        }
        return read;
    }

    void send(final Encoder encoder)
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
            DebugLogger.log(FIX_TEST, "> [" + writeAsciiBuffer.getAscii(offset, length) + "]");
            writeBuffer.clear();
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    LogonDecoder readLogonReply()
    {
        return readMessage(new LogonDecoder());
    }

    void testRequest(final String testReqID)
    {
        setupHeader(testRequestEncoder.header(), msgSeqNum++, false);
        testRequestEncoder.testReqID(testReqID);
        send(testRequestEncoder);
    }

    void readHeartbeat(final String testReqID)
    {
        final HeartbeatDecoder heartbeat = readMessage(new HeartbeatDecoder());
        assertTrue(heartbeat.hasTestReqID());
        assertEquals(testReqID, heartbeat.testReqIDAsString());
    }

    public void close()
    {
        CloseHelper.close(socket);
    }

    void logoutAndAwaitReply()
    {
        logout();

        final LogoutDecoder logout = readMessage(new LogoutDecoder());

        assertFalse(logout.textAsString(), logout.hasText());
    }

    public void sendExecutionReport(final int msgSeqNum, final boolean possDupFlag)
    {
        final ExecutionReportEncoder executionReportEncoder = new ExecutionReportEncoder();
        final HeaderEncoder header = executionReportEncoder.header();

        setupHeader(header, msgSeqNum, possDupFlag);

        executionReportEncoder
            .orderID("order")
            .execID("exec")
            .execType(ExecType.FILL)
            .ordStatus(OrdStatus.FILLED)
            .side(Side.BUY);

        executionReportEncoder.instrument().symbol("IBM");

        send(executionReportEncoder);
    }
}
