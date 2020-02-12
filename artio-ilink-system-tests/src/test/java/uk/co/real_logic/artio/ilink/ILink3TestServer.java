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

import iLinkBinary.*;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.sbe.MessageDecoderFlyweight;
import org.agrona.sbe.MessageEncoderFlyweight;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.system_tests.TestSystem;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static iLinkBinary.NegotiationResponse501Encoder.credentialsHeaderLength;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.ilink.ILink3Proxy.ILINK_HEADER_LENGTH;
import static uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader.*;

public class ILink3TestServer
{
    private static final int BUFFER_SIZE = 8 * 1024;

    private final SocketChannel socket;
    private final ByteBuffer writeBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final UnsafeBuffer unsafeWriteBuffer = new UnsafeBuffer(writeBuffer);
    private final ByteBuffer readBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private final UnsafeBuffer unsafeReadBuffer = new UnsafeBuffer(readBuffer);

    private final MessageHeaderDecoder iLinkHeaderDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder iLinkHeaderEncoder = new MessageHeaderEncoder();
    private final TestSystem testSystem;

    private long uuid;
    private long establishRequestTimestamp;
    private long negotiateRequestTimestamp;
    private int requestedKeepAliveInterval;

    public ILink3TestServer(
        final int port,
        final Runnable connectOperation,
        final TestSystem testSystem) throws IOException
    {
        this.testSystem = testSystem;
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

        iLinkHeaderDecoder.wrap(unsafeReadBuffer, SOFH_LENGTH);
        iLinkHeaderEncoder.wrap(unsafeWriteBuffer, SOFH_LENGTH);
    }

    public <T extends MessageDecoderFlyweight> T read(final T messageDecoder)
    {
        return testSystem.awaitBlocking(() ->
        {
            try
            {
                final int read = socket.read(readBuffer);
                final int totalLength = readSofh(unsafeReadBuffer, 0);
                if (totalLength != read)
                {
                    throw new IllegalArgumentException("totalLength=" + totalLength + ",read=" + read);
                }
                final int messageOffset = iLinkHeaderDecoder.encodedLength() + SOFH_LENGTH;
                final int blockLength = iLinkHeaderDecoder.blockLength();
                messageDecoder.wrap(
                    unsafeReadBuffer,
                    messageOffset,
                    blockLength,
                    iLinkHeaderDecoder.version());

                assertEquals(messageDecoder.sbeTemplateId(), iLinkHeaderDecoder.templateId());
                assertThat(read, greaterThanOrEqualTo(messageOffset + blockLength));

                readBuffer.clear();

                return messageDecoder;
            }
            catch (final IOException e)
            {
                LangUtil.rethrowUnchecked(e);
                return null;
            }
        });
    }

    public void wrap(final MessageEncoderFlyweight messageEncoder, final int length)
    {
        final int messageSize = ILINK_HEADER_LENGTH + length;
        writeSofh(unsafeWriteBuffer, 0, messageSize);

        iLinkHeaderEncoder
            .wrap(unsafeWriteBuffer, SOFH_LENGTH)
            .blockLength(messageEncoder.sbeBlockLength())
            .templateId(messageEncoder.sbeTemplateId())
            .schemaId(messageEncoder.sbeSchemaId())
            .version(messageEncoder.sbeSchemaVersion());

        messageEncoder.wrap(unsafeWriteBuffer, ILINK_HEADER_LENGTH);
    }

    public void write()
    {
        final int messageSize = readSofhMessageSize(unsafeWriteBuffer, 0);
        writeBuffer.position(0).limit(messageSize);

        testSystem.awaitBlocking(() ->
        {
            try
            {

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

    public void readNegotiate(final String expectedAccessKeyId, final String expectedFirmId)
    {
        final Negotiate500Decoder negotiate = read(new Negotiate500Decoder());
        assertEquals(expectedAccessKeyId, negotiate.accessKeyID());

        assertEquals(expectedFirmId, negotiate.firm());
        assertEquals(0, negotiate.credentialsLength());
        DebugLogger.log(LogTag.FIX_TEST, negotiate.toString());

        uuid = negotiate.uUID();
        negotiateRequestTimestamp = negotiate.requestTimestamp();
    }

    public long uuid()
    {
        return uuid;
    }

    public void writeNegotiateResponse()
    {
        final NegotiationResponse501Encoder negotiateResponse = new NegotiationResponse501Encoder();
        wrap(negotiateResponse, NegotiationResponse501Encoder.BLOCK_LENGTH + credentialsHeaderLength());

        negotiateResponse
            .uUID(uuid)
            .requestTimestamp(negotiateRequestTimestamp)
            .secretKeySecureIDExpiration(1)
            .faultToleranceIndicator(FTI.Primary)
            .splitMsg(SplitMsg.NULL_VAL)
            .previousSeqNo(0)
            .previousUUID(0);

        write();
    }

    public void readEstablish(
        final String expectedAccessKeyID, final String expectedFirmId, final String expectedSessionId,
        final int expectedKeepAliveInterval)
    {
        final Establish503Decoder establish = read(new Establish503Decoder());
        //  establish.hMACSignature()
        assertEquals(expectedAccessKeyID, establish.accessKeyID());
        // TradingSystemInfo

        final long uuid = establish.uUID();
        assertEquals(this.uuid, uuid);

        establishRequestTimestamp = establish.requestTimestamp();
        assertThat(establishRequestTimestamp, greaterThanOrEqualTo(negotiateRequestTimestamp));
        final long nextSeqNo = establish.nextSeqNo();
        assertThat(nextSeqNo, greaterThanOrEqualTo(1L));

        assertEquals(expectedSessionId, establish.session());
        assertEquals(expectedFirmId, establish.firm());
        requestedKeepAliveInterval = establish.keepAliveInterval();
        assertEquals(expectedKeepAliveInterval, requestedKeepAliveInterval);
    }

    public void writeEstablishAck()
    {
        final EstablishmentAck504Encoder establishmentAck = new EstablishmentAck504Encoder();
        wrap(establishmentAck, EstablishmentAck504Encoder.BLOCK_LENGTH);

        establishmentAck
            .uUID(uuid)
            .requestTimestamp(establishRequestTimestamp)
            .nextSeqNo(1)
            .previousSeqNo(0)
            .previousUUID(0)
            .keepAliveInterval(requestedKeepAliveInterval + 100)
            .secretKeySecureIDExpiration(1)
            .faultToleranceIndicator(FTI.Primary)
            .splitMsg(SplitMsg.NULL_VAL);

        write();
    }
}
