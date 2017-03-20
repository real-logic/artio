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
package uk.co.real_logic.fix_gateway.engine.framer;

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.replication.ClusterFragmentHandler;
import uk.co.real_logic.fix_gateway.replication.ClusterHeader;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

class SenderEndPoints implements AutoCloseable, ControlledFragmentHandler, ClusterFragmentHandler
{
    private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final Long2ObjectHashMap<SenderEndPoint> connectionIdToSenderEndpoint = new Long2ObjectHashMap<>();
    private final ErrorHandler errorHandler;
    private long timeInMs;

    SenderEndPoints(final ErrorHandler errorHandler)
    {
        this.errorHandler = errorHandler;
    }

    public void add(final SenderEndPoint senderEndPoint)
    {
        connectionIdToSenderEndpoint.put(senderEndPoint.connectionId(), senderEndPoint);
    }

    void removeConnection(final long connectionId)
    {
        final SenderEndPoint senderEndPoint = connectionIdToSenderEndpoint.remove(connectionId);
        if (senderEndPoint != null)
        {
            senderEndPoint.close();
        }
    }

    void onMessage(
        final int libraryId,
        final long connectionId,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long position)
    {
        final SenderEndPoint endPoint = connectionIdToSenderEndpoint.get(connectionId);
        if (endPoint != null)
        {
            endPoint.onNormalFramedMessage(libraryId, buffer, offset, length, position, timeInMs);
        }
    }

    Action onReplayMessage(
        final long sessionId, final DirectBuffer buffer, final int offset, final int length)
    {
        for (final SenderEndPoint endPoint : connectionIdToSenderEndpoint.values())
        {
            if (endPoint.sessionId() == sessionId)
            {
                return endPoint.onReplayFramedMessage(buffer, offset, length, timeInMs);
            }
        }

        errorHandler.onError(new IllegalArgumentException(String.format(
            "Failed to replay message on %1$d [%2$s]",
            sessionId,
            buffer.getStringWithoutLengthUtf8(offset, length))));

        return CONTINUE;
    }

    @SuppressWarnings("FinalParameters")
    public Action onFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        return onSlowConsumerMessageFragment(buffer, offset, length, header.position());
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final ClusterHeader header)
    {
        return onSlowConsumerMessageFragment(buffer, offset, length, header.position());
    }

    @SuppressWarnings("FinalParameters")
    private Action onSlowConsumerMessageFragment(
        final DirectBuffer buffer,
        int offset,
        final int length,
        final long position)
    {
        messageHeader.wrap(buffer, offset);

        if (messageHeader.templateId() == FixMessageDecoder.TEMPLATE_ID)
        {
            offset += HEADER_LENGTH;
            fixMessage.wrap(buffer, offset, messageHeader.blockLength(), messageHeader.version());
            final long connectionId = fixMessage.connection();

            final SenderEndPoint senderEndPoint = connectionIdToSenderEndpoint.get(connectionId);
            if (senderEndPoint != null)
            {
                final int bodyLength = fixMessage.bodyLength();
                final int libraryId = fixMessage.libraryId();
                return senderEndPoint.onSlowConsumerMessageFragment(
                    buffer, offset, length - HEADER_LENGTH, position, bodyLength, libraryId, timeInMs);
            }
        }

        return CONTINUE;
    }

    public void close()
    {
        connectionIdToSenderEndpoint
            .values()
            .forEach(SenderEndPoint::close);
    }

    void timeInMs(final long timeInMs)
    {
        this.timeInMs = timeInMs;
    }

    public int checkTimeouts(final long timeInMs)
    {
        for (final SenderEndPoint senderEndPoint : connectionIdToSenderEndpoint.values())
        {
            if (senderEndPoint.checkTimeouts(timeInMs))
            {
                return 1;
            }
        }

        return 0;
    }
}
