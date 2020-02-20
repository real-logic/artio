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
package uk.co.real_logic.artio.engine.framer;

import io.aeron.ExclusivePublication;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.engine.ByteBufferUtil;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.ILinkMessageEncoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;

import java.nio.channels.ClosedChannelException;

import static uk.co.real_logic.artio.ilink.AbstractILink3Proxy.ARTIO_HEADER_LENGTH;
import static uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader.SOFH_LENGTH;
import static uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader.readSofh;

class ILink3ReceiverEndPoint extends ReceiverEndPoint
{
    private final UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[ARTIO_HEADER_LENGTH]);
    private final ExclusivePublication inboundPublication;

    ILink3ReceiverEndPoint(
        final long connectionId,
        final TcpChannel channel,
        final int bufferSize,
        final ErrorHandler errorHandler,
        final Framer framer,
        final ExclusivePublication inboundPublication)
    {
        super(channel, connectionId, bufferSize, errorHandler, framer);
        this.inboundPublication = inboundPublication;

        makeHeader();
    }

    private void makeHeader()
    {
        final ILinkMessageEncoder iLinkMessage = new ILinkMessageEncoder();
        final MessageHeaderEncoder header = new MessageHeaderEncoder();

        iLinkMessage
            .wrapAndApplyHeader(headerBuffer, 0, header)
            .connection(connectionId);
    }

    void removeEndpointFromFramer()
    {
        // TODO
    }

    void disconnectEndpoint(final DisconnectReason reason)
    {
        // TODO
    }

    boolean retryFrameMessages()
    {
        // TODO
        return false;
    }

    int poll()
    {
        try
        {
            final int bytesRead = readData();
            if (bytesRead > 0 && !frameMessages())
            {
                return -bytesRead;
            }

            return bytesRead;
        }
        catch (final ClosedChannelException ex)
        {
            onDisconnectDetected();
            return 1;
        }
        catch (final Exception ex)
        {
            // Regular disconnects aren't errors
            if (!Exceptions.isJustDisconnect(ex))
            {
                errorHandler.onError(ex);
            }

            onDisconnectDetected();
            return 1;
        }
    }

    // false iff back pressured
    private boolean frameMessages()
    {
        int offset = 0;
        while (usedBufferData > SOFH_LENGTH)
        {
            final int messageSize = readSofh(buffer, offset);
            if ((offset + messageSize) > usedBufferData)
            {
                moveRemainingDataToBufferStart(offset);
                return true;
            }

            final long position = inboundPublication.offer(
                headerBuffer,
                0,
                ARTIO_HEADER_LENGTH,
                buffer,
                offset,
                messageSize);

            if (position < 0)
            {
                moveRemainingDataToBufferStart(offset);
                return false;
            }

            usedBufferData -= messageSize;
            offset += messageSize;
        }

        moveRemainingDataToBufferStart(offset);
        return true;
    }

    private void moveRemainingDataToBufferStart(final int offset)
    {
        if (usedBufferData > 0)
        {
            buffer.putBytes(0, buffer, offset, usedBufferData);
        }
        // position set to ensure that back pressure is applied to TCP when read(byteBuffer) called.
        ByteBufferUtil.position(byteBuffer, usedBufferData);
    }

    boolean requiresAuthentication()
    {
        return false;
    }

    void closeResources()
    {
        try
        {
            channel.close();
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }
    }
}
