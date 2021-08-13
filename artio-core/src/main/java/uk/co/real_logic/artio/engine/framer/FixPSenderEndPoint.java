/*
 * Copyright 2020-2021 Monotonic Ltd.
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
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.engine.ByteBufferUtil;
import uk.co.real_logic.artio.engine.MessageTimingHandler;

import java.io.IOException;
import java.nio.ByteBuffer;

import static uk.co.real_logic.artio.LogTag.FIX_MESSAGE_TCP;

abstract class FixPSenderEndPoint extends SenderEndPoint
{
    protected static final int NO_REATTEMPT = 0;

    protected final TcpChannel channel;
    protected final ErrorHandler errorHandler;

    protected int reattemptBytesWritten = NO_REATTEMPT;

    static FixPSenderEndPoint of(
        final long connectionId,
        final TcpChannel channel,
        final ErrorHandler errorHandler,
        final ExclusivePublication inboundPublication,
        final int libraryId,
        final MessageTimingHandler messageTimingHandler,
        final boolean explicitSequenceNumbers,
        final int templateIdOffset,
        final int retransmissionTemplateId,
        final FixPSenderEndPoints fixPSenderEndPoints)
    {
        if (explicitSequenceNumbers)
        {
            return new ExplicitFixPSenderEndPoint(
                connectionId, channel, errorHandler, inboundPublication, libraryId, messageTimingHandler);
        }
        else
        {
            return new ImplicitFixPSenderEndPoint(
                connectionId, channel, errorHandler, inboundPublication, libraryId,
                templateIdOffset, retransmissionTemplateId, fixPSenderEndPoints);
        }
    }

    FixPSenderEndPoint(
        final long connectionId,
        final TcpChannel channel,
        final ErrorHandler errorHandler,
        final ExclusivePublication inboundPublication,
        final int libraryId)
    {
        super(connectionId, inboundPublication, libraryId);
        this.channel = channel;
        this.errorHandler = errorHandler;
    }

    public abstract Action onMessage(DirectBuffer directBuffer, int offset, boolean retransmit);

    protected int writeBuffer(
        final DirectBuffer directBuffer, final int offset, final int messageSize) throws IOException
    {
        final int reattemptBytesWritten = this.reattemptBytesWritten;

        final ByteBuffer buffer = directBuffer.byteBuffer();
        final int startLimit = buffer.limit();
        final int startPosition = buffer.position();

        ByteBufferUtil.limit(buffer, offset + messageSize);
        ByteBufferUtil.position(buffer, reattemptBytesWritten + offset);

        final int written = channel.write(buffer);
        if (written > 0)
        {
            ByteBufferUtil.position(buffer, offset);
            DebugLogger.logBytes(FIX_MESSAGE_TCP, "Written  ", buffer, startPosition, written);

            buffer.limit(startLimit).position(startPosition);
        }

        return reattemptBytesWritten + written;
    }

    abstract boolean reattempt();

}
