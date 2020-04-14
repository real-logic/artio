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
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.engine.ByteBufferUtil;
import uk.co.real_logic.artio.ilink.SimpleOpenFramingHeader;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;
import uk.co.real_logic.artio.messages.ReplayCompleteEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.LogTag.FIX_MESSAGE_TCP;

public class ILink3SenderEndPoint
{
    private static final int NO_REATTEMPT = 0;

    private static final int REPLAY_COMPLETE_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + ReplayCompleteEncoder.BLOCK_LENGTH;

    private final MessageHeaderEncoder messageHeader = new MessageHeaderEncoder();
    private final ReplayCompleteEncoder replayComplete = new ReplayCompleteEncoder();
    private final BufferClaim bufferClaim = new BufferClaim();

    private final long connectionId;
    private final TcpChannel channel;
    private final ErrorHandler errorHandler;
    private final ExclusivePublication inboundPublication;
    private final int libraryId;

    private int reattemptBytesWritten = NO_REATTEMPT;

    public ILink3SenderEndPoint(
        final long connectionId,
        final TcpChannel channel,
        final ErrorHandler errorHandler,
        final ExclusivePublication inboundPublication,
        final int libraryId)
    {
        this.connectionId = connectionId;
        this.channel = channel;
        this.errorHandler = errorHandler;
        this.inboundPublication = inboundPublication;
        this.libraryId = libraryId;
    }

    public Action onMessage(final DirectBuffer directBuffer, final int offset)
    {
        final int messageSize = SimpleOpenFramingHeader.readSofhMessageSize(directBuffer, offset);
        final int reattemptBytesWritten = this.reattemptBytesWritten;

        final ByteBuffer buffer = directBuffer.byteBuffer();
        final int startLimit = buffer.limit();
        final int startPosition = buffer.position();

        ByteBufferUtil.limit(buffer, offset + messageSize);
        ByteBufferUtil.position(buffer, reattemptBytesWritten + offset);

        try
        {
            final int written = channel.write(buffer);
            if (written > 0)
            {
                ByteBufferUtil.position(buffer, offset);
                DebugLogger.logBytes(FIX_MESSAGE_TCP, "Written  ", buffer, startPosition, written);

                buffer.limit(startLimit).position(startPosition);
            }
            final int totalWritten = reattemptBytesWritten + written;
            if (totalWritten < messageSize)
            {
                this.reattemptBytesWritten = totalWritten;

                return ABORT;
            }
            else
            {
                this.reattemptBytesWritten = NO_REATTEMPT;
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

    public Action onReplayComplete(final long connectionId)
    {
        final BufferClaim bufferClaim = this.bufferClaim;
        final long position = inboundPublication.tryClaim(REPLAY_COMPLETE_LENGTH, bufferClaim);

        if (Pressure.isBackPressured(position))
        {
            return ABORT;
        }

        replayComplete
            .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeader)
            .connection(connectionId)
            .libraryId(libraryId);

        bufferClaim.commit();

        return CONTINUE;
    }
}
