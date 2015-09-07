package uk.co.real_logic.fix_gateway.system_benchmarks;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.CommonConfiguration;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.builder.LogonEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.LockSupport;

import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.util.concurrent.TimeUnit.SECONDS;
import static uk.co.real_logic.fix_gateway.system_benchmarks.Configuration.*;
import static uk.co.real_logic.fix_gateway.util.AsciiFlyweight.UNKNOWN_INDEX;

public abstract class AbstractBenchmarkClient
{
    protected static final String HOST = System.getProperty("fix.benchmark.host", "localhost");
    protected static final int BUFFER_SIZE = 16 * 1024;
    protected static final byte NINE = (byte) '9';

    protected final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    protected final MutableAsciiFlyweight writeFlyweight =
        new MutableAsciiFlyweight(new UnsafeBuffer(writeBuffer));
    protected final ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    protected final MutableAsciiFlyweight readFlyweight =
        new MutableAsciiFlyweight(new UnsafeBuffer(readBuffer));

    protected boolean lastWasSep;

    protected TestRequestEncoder setupTestRequest()
    {
        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest
            .header()
            .senderCompID(INITIATOR_ID)
            .targetCompID(ACCEPTOR_ID);
        testRequest.testReqID("a");
        return testRequest;
    }

    protected void logon(final SocketChannel socketChannel) throws IOException
    {
        final LogonEncoder logon = new LogonEncoder();
        logon.heartBtInt(10);
        logon
            .header()
            .sendingTime(System.currentTimeMillis())
            .senderCompID(INITIATOR_ID)
            .targetCompID(ACCEPTOR_ID)
            .msgSeqNum(1);

        write(socketChannel, logon.encode(writeFlyweight, 0));

        final int length = read(socketChannel);
        final LogonDecoder logonDecoder = new LogonDecoder();
        logonDecoder.decode(readFlyweight, 0, length);
        System.out.println("Authenticated: " + logonDecoder);
    }

    protected void write(final SocketChannel socketChannel, final int amount) throws IOException
    {
        writeBuffer.position(0);
        writeBuffer.limit(amount);
        int remaining = amount;
        do
        {
            remaining -= socketChannel.write(writeBuffer);
        }
        while (remaining > 0);
    }

    protected int read(final SocketChannel socketChannel) throws IOException
    {
        readBuffer.clear();
        int length;
        do
        {
            length = socketChannel.read(readBuffer);
            LockSupport.parkNanos(1);
        }
        while (length == 0);
        // System.out.printf("Read Data: %d\n", length);
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

    protected static void printTimes(final long startTime)
    {
        final long duration = System.currentTimeMillis() - startTime;
        final double rate = (double) CommonConfiguration.MESSAGES_EXCHANGED / duration;
        System.out.printf("%d messages in %d ms\n", CommonConfiguration.MESSAGES_EXCHANGED, duration);
        System.out.printf("%G messages / ms\n", rate);
        System.out.printf("%G messages / s\n", rate * 1000.0);
    }

    protected int scanForReceivesMessages(
        final MutableAsciiFlyweight readFlyweight,
        final int length)
    {
        int messagesReceived = 0;

        if (length > 0)
        {
            int index = 0;

            while (index < length)
            {
                index = readFlyweight.scan(index, length - 1, NINE);

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

    protected static boolean isSeparator(final MutableAsciiFlyweight readFlyweight, final int index)
    {
        return readFlyweight.getChar(index) == '\001';
    }

    protected int encode(final TestRequestEncoder testRequest, final HeaderEncoder header, final int seqNum)
    {
        header
            .sendingTime(System.currentTimeMillis())
            .msgSeqNum(seqNum);

        return testRequest.encode(writeFlyweight, 0);
    }
}
