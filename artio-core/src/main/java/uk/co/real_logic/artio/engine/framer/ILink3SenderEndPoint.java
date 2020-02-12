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

import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.engine.ByteBufferUtil;
import uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.LogTag.FIX_MESSAGE_TCP;

public class ILink3SenderEndPoint
{
    private final long connectionId;
    private final TcpChannel channel;
    private final ErrorHandler errorHandler;

    public ILink3SenderEndPoint(final long connectionId, final TcpChannel channel, final ErrorHandler errorHandler)
    {
        this.connectionId = connectionId;
        this.channel = channel;
        this.errorHandler = errorHandler;
    }

    public ControlledFragmentHandler.Action onMessage(final DirectBuffer directBuffer, final int offset)
    {
        final int messageSize = SimpleOpenFramingHeader.readSofhMessageSize(directBuffer, offset);

        final ByteBuffer buffer = directBuffer.byteBuffer();
        final int startLimit = buffer.limit();
        final int startPosition = buffer.position();

        ByteBufferUtil.limit(buffer, offset + messageSize);
        ByteBufferUtil.position(buffer, offset);

        try
        {
            final int written = channel.write(buffer);
            if (written > 0)
            {
                ByteBufferUtil.position(buffer, offset);
                DebugLogger.log(FIX_MESSAGE_TCP, "Written  ", buffer, written);

                buffer.limit(startLimit).position(startPosition);
            }
            if (written != messageSize)
            {
                // TODO: better handling of partial sends.
                // We could just keep track of partial sends and backpressure
                System.err.println("Failed to send some data");
            }
        }
        catch (final IOException e)
        {
            errorHandler.onError(e);
        }

        return CONTINUE;
    }

    public long connectionId()
    {
        return connectionId;
    }
}
