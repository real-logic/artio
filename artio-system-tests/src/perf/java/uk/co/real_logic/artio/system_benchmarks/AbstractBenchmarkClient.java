/*
 * Copyright 2015-2025 Real Logic Limited.
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
package uk.co.real_logic.artio.system_benchmarks;

import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.builder.HeaderEncoder;
import uk.co.real_logic.artio.builder.LogonEncoder;
import uk.co.real_logic.artio.builder.TestRequestEncoder;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.engine.ByteBufferUtil;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.LockSupport;

import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.util.concurrent.TimeUnit.SECONDS;
import static uk.co.real_logic.artio.system_benchmarks.BenchmarkConfiguration.*;
import static uk.co.real_logic.artio.util.AsciiBuffer.UNKNOWN_INDEX;

public abstract class AbstractBenchmarkClient
{
    protected static final String HOST = System.getProperty("fix.benchmark.host", "localhost");
    protected static final int BUFFER_SIZE = 16 * 1024;
    protected static final byte NINE = (byte)'9';

    protected final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
    protected final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    protected final MutableAsciiBuffer writeFlyweight =
        new MutableAsciiBuffer(writeBuffer);
    protected final ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    protected final MutableAsciiBuffer readFlyweight =
        new MutableAsciiBuffer(readBuffer);

    protected boolean lastWasSep;

    protected TestRequestEncoder setupTestRequest()
    {
        return setupTestRequest(INITIATOR_ID);
    }

    protected TestRequestEncoder setupTestRequest(final String initiatorId)
    {
        final TestRequestEncoder testRequest = new TestRequestEncoder();
        setupHeader(initiatorId, testRequest.header());
        testRequest.testReqID("a");
        return testRequest;
    }

    protected void logon(final SocketChannel socketChannel) throws IOException
    {
        logon(socketChannel, INITIATOR_ID, 10);
    }

    protected LogonDecoder logon(final SocketChannel socketChannel, final String initiatorId, final int heartBtInt)
        throws IOException
    {
        final LogonEncoder logon = new LogonEncoder();
        logon.heartBtInt(heartBtInt);
        setupHeader(initiatorId, logon.header())
            .msgSeqNum(1);

        timestampEncoder.encode(System.currentTimeMillis());

        write(socketChannel, logon.encode(writeFlyweight, 0));

        final int length = read(socketChannel);
        final LogonDecoder logonDecoder = new LogonDecoder();
        logonDecoder.decode(readFlyweight, 0, length);
        return logonDecoder;
    }

    protected HeaderEncoder setupHeader(final String initiatorId, final HeaderEncoder header)
    {
        return header
            .sendingTime(timestampEncoder.buffer())
            .senderCompID(initiatorId)
            .targetCompID(ACCEPTOR_ID);
    }

    protected void write(final SocketChannel socketChannel, final long result) throws IOException
    {
        final int offset = Encoder.offset(result);
        final int length = Encoder.length(result);

        ByteBufferUtil.position(writeBuffer, offset);
        ByteBufferUtil.limit(writeBuffer, offset + length);
        int remaining = length;
        do
        {
            remaining -= socketChannel.write(writeBuffer);
        }
        while (remaining > 0);
        // System.out.println(writeFlyweight.getAscii(0, amount));
    }

    protected int read(final SocketChannel socketChannel) throws IOException
    {
        readBuffer.clear();
        int length;
        do
        {
            length = socketChannel.read(readBuffer);
        }
        while (length == 0);
        return length;
    }

    protected void parkAfterWarmup()
    {
        LockSupport.parkNanos(SECONDS.toNanos(1));
    }

    protected SocketChannel open() throws IOException
    {
        final SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(HOST, PORT));
        socketChannel.configureBlocking(false);
        socketChannel.setOption(TCP_NODELAY, true);
        socketChannel.setOption(SO_RCVBUF, 1024 * 1024);
        socketChannel.setOption(SO_RCVBUF, 1024 * 1024);
        return socketChannel;
    }

    protected static void printThroughput(final long startTime, final int messagesExchanged)
    {
        final long duration = System.currentTimeMillis() - startTime;
        final double rate = (double)messagesExchanged / duration;
        System.out.printf("%d messages in %d ms%n", messagesExchanged, duration);
        System.out.printf("%G messages / s%n", rate * 1000.0);
    }

    protected int scanForReceivesMessages(
        final MutableAsciiBuffer readFlyweight,
        final int length)
    {
        int messagesReceived = 0;

        if (length > 0)
        {
            int index = 0;

            while (index < length)
            {
                index = readFlyweight.scan(index, length, NINE);

                if (index == UNKNOWN_INDEX)
                {
                    break;
                }

                if (index == 0)
                {
                    if (lastWasSep)
                    {
                        messagesReceived++;
                    }
                }
                else if (isSeparator(readFlyweight, index - 1))
                {
                    messagesReceived++;
                }

                index += 1;
            }

            lastWasSep = isSeparator(readFlyweight, length - 1);
        }

        return messagesReceived;
    }

    protected static boolean isSeparator(final MutableAsciiBuffer readFlyweight, final int index)
    {
        return readFlyweight.getChar(index) == '\001';
    }

    protected long encode(final Encoder encoder, final HeaderEncoder header, final int seqNum)
    {
        return encode(encoder, header, seqNum, 0);
    }

    protected long encode(
        final Encoder encoder,
        final HeaderEncoder header,
        final int seqNum,
        final int offset)
    {
        header.msgSeqNum(seqNum);
        timestampEncoder.encode(System.currentTimeMillis());

        return encoder.encode(writeFlyweight, offset);
    }
}
