/*
 * Copyright 2015-2022 Real Logic Limited.
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
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.util.CharFormatter;

import java.util.function.LongToIntFunction;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.DebugLogger.IS_REPLAY_LOG_TAG_ENABLED;
import static uk.co.real_logic.artio.messages.ThrottleRejectDecoder.businessRejectRefIDHeaderLength;

class FixSenderEndPoints implements AutoCloseable, ControlledFragmentHandler
{
    private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;

    final CharFormatter missReplayComplete = new CharFormatter(
        "SEPs.missReplayComplete, connId=%s, corrId=%s, slow=%s");

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final StartReplayDecoder startReplay = new StartReplayDecoder();
    private final ValidResendRequestDecoder validResendRequestDecoder = new ValidResendRequestDecoder();
    private final ThrottleRejectDecoder throttleReject = new ThrottleRejectDecoder();
    private final Long2ObjectHashMap<FixSenderEndPoint> connectionIdToSenderEndpoint = new Long2ObjectHashMap<>();
    private final ErrorHandler errorHandler;
    private final LongToIntFunction libraryLookup = this::libraryLookup;

    private int libraryLookup(final long sessionId)
    {
        for (final FixSenderEndPoint senderEndPoint : connectionIdToSenderEndpoint.values())
        {
            if (senderEndPoint.sessionId() == sessionId)
            {
                return senderEndPoint.libraryId();
            }
        }

        return FixEngine.ENGINE_LIBRARY_ID;
    }

    private long timeInMs;

    FixSenderEndPoints(final ErrorHandler errorHandler)
    {
        this.errorHandler = errorHandler;
    }

    public void add(final FixSenderEndPoint senderEndPoint)
    {
        connectionIdToSenderEndpoint.put(senderEndPoint.connectionId(), senderEndPoint);
    }

    void removeConnection(final long connectionId)
    {
        final FixSenderEndPoint senderEndPoint = connectionIdToSenderEndpoint.remove(connectionId);
        if (senderEndPoint != null)
        {
            senderEndPoint.close();
        }
    }

    boolean onMessage(
        final int libraryId,
        final long connectionId,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int sequenceNumber,
        final Header header,
        final int metaDataLength)
    {
        final FixSenderEndPoint endPoint = connectionIdToSenderEndpoint.get(connectionId);
        if (endPoint != null)
        {
            endPoint.onOutboundMessage(
                libraryId, buffer, offset, length, sequenceNumber, header, timeInMs, metaDataLength);
            return true;
        }

        return false;
    }

    Action onThrottleReject(
        final int libraryId,
        final long connectionId,
        final long refMsgType,
        final int refSeqNum,
        final int sequenceNumber,
        final DirectBuffer businessRejectRefIDBuffer,
        final int businessRejectRefIDOffset,
        final int businessRejectRefIDLength,
        final Header header)
    {
        final FixSenderEndPoint endPoint = connectionIdToSenderEndpoint.get(connectionId);
        if (endPoint != null)
        {
            endPoint.onThrottleReject(
                libraryId, refMsgType, refSeqNum, sequenceNumber,
                businessRejectRefIDBuffer, businessRejectRefIDOffset, businessRejectRefIDLength, header,
                timeInMs);
        }

        return null;
    }

    Action onReplayMessage(
        final long connectionId, final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final FixSenderEndPoint endPoint = connectionIdToSenderEndpoint.get(connectionId);
        if (endPoint != null)
        {
            return endPoint.onReplayMessage(buffer, offset, length, timeInMs, header);
        }
        else
        {
            logReplayError(connectionId, buffer, offset, length);

            return CONTINUE;
        }
    }

    Action onSlowReplayMessage(
        final long connectionId,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header,
        final int metaDataLength)
    {
        final FixSenderEndPoint endPoint = connectionIdToSenderEndpoint.get(connectionId);
        if (endPoint != null)
        {
            return endPoint.onSlowReplayMessage(buffer, offset, length, timeInMs, header, metaDataLength);
        }
        else
        {
            // We don't log the replay error at this point, as it will likely be a message that has already been
            // attempted. This cannot be a slow endpoint anymore - it's a disconnected endpoint.

            return CONTINUE;
        }
    }

    private void logReplayError(final long connectionId, final DirectBuffer buffer, final int offset, final int length)
    {
        errorHandler.onError(new IllegalArgumentException(String.format(
            "Failed to replay message on conn=%1$d [%2$s], this probably indicates the connection has disconnected " +
            "from Artio whilst this message was in the process of being replayed",
            connectionId,
            buffer.getStringWithoutLengthUtf8(offset, length))));
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        return onSlowConsumerMessageFragment(buffer, offset, length, header);
    }

    @SuppressWarnings("FinalParameters")
    private Action onSlowConsumerMessageFragment(
        final DirectBuffer buffer,
        int offset,
        final int length,
        final Header header)
    {
        final MessageHeaderDecoder messageHeader = this.messageHeader;
        messageHeader.wrap(buffer, offset);

        final int templateId = messageHeader.templateId();
        if (templateId == FixMessageDecoder.TEMPLATE_ID)
        {
            offset += HEADER_LENGTH;
            final int version = messageHeader.version();
            fixMessage.wrap(buffer, offset, messageHeader.blockLength(), version);

            final long connectionId = fixMessage.connection();
            final FixSenderEndPoint senderEndPoint = connectionIdToSenderEndpoint.get(connectionId);
            if (senderEndPoint != null)
            {
                final int metaDataLength = fixMessage.skipMetaData();

                final int bodyLength = fixMessage.bodyLength();
                final int libraryId = fixMessage.libraryId();
                final int sequenceNumber = fixMessage.sequenceNumber();
                return senderEndPoint.onSlowOutboundMessage(
                    buffer, offset, length - HEADER_LENGTH, header, bodyLength, libraryId, timeInMs,
                    metaDataLength, sequenceNumber);
            }
        }
        else if (templateId == ThrottleRejectDecoder.TEMPLATE_ID)
        {
            offset += HEADER_LENGTH;
            final int version = messageHeader.version();
            throttleReject.wrap(buffer, offset, messageHeader.blockLength(), version);

            final long connectionId = throttleReject.connection();
            final FixSenderEndPoint senderEndPoint = connectionIdToSenderEndpoint.get(connectionId);
            if (senderEndPoint != null)
            {
                final int businessRejectRefIDOffset = throttleReject.limit() + businessRejectRefIDHeaderLength();
                return senderEndPoint.onSlowThrottleReject(
                    throttleReject.libraryId(),
                    throttleReject.refMsgType(),
                    throttleReject.refSeqNum(),
                    throttleReject.sequenceNumber(),
                    buffer,
                    businessRejectRefIDOffset,
                    throttleReject.businessRejectRefIDLength(),
                    header,
                    timeInMs);
            }
        }
        else if (templateId == StartReplayDecoder.TEMPLATE_ID)
        {
            offset += HEADER_LENGTH;
            final int version = messageHeader.version();
            startReplay.wrap(buffer, offset, messageHeader.blockLength(), version);

            final long connectionId = startReplay.connection();
            final long correlationId = startReplay.correlationId();
            onStartReplay(connectionId, correlationId, header.position(), true);
        }
        else if (templateId == ValidResendRequestDecoder.TEMPLATE_ID)
        {
            offset += HEADER_LENGTH;
            final int version = messageHeader.version();
            validResendRequestDecoder.wrap(buffer, offset, messageHeader.blockLength(), version);

            final long connectionId = validResendRequestDecoder.connection();
            final long correlationId = validResendRequestDecoder.correlationId();
            onValidResendRequest(connectionId, correlationId, true);
        }

        return CONTINUE;
    }

    Action onReplayComplete(final long connectionId, final long correlationId, final boolean slow)
    {
        final FixSenderEndPoint senderEndPoint = connectionIdToSenderEndpoint.get(connectionId);
        if (senderEndPoint != null)
        {
            return senderEndPoint.onReplayComplete(correlationId, slow);
        }
        else
        {
            if (IS_REPLAY_LOG_TAG_ENABLED)
            {
                DebugLogger.log(LogTag.REPLAY, missReplayComplete.clear().with(connectionId).with(correlationId)
                    .with(slow));
            }
        }
        return CONTINUE;
    }

    public void close()
    {
        connectionIdToSenderEndpoint
            .values()
            .forEach(FixSenderEndPoint::close);
    }

    public boolean isSlowConsumer(final long connectionId)
    {
        final FixSenderEndPoint fixSenderEndPoint = connectionIdToSenderEndpoint.get(connectionId);
        return fixSenderEndPoint != null && fixSenderEndPoint.isSlowConsumer();
    }

    void timeInMs(final long timeInMs)
    {
        this.timeInMs = timeInMs;
    }

    int checkTimeouts(final long timeInMs)
    {
        int count = 0;
        for (final FixSenderEndPoint senderEndPoint : connectionIdToSenderEndpoint.values())
        {
            if (senderEndPoint.checkTimeouts(timeInMs))
            {
                count++;
            }
        }

        return count;
    }

    LongToIntFunction libraryLookup()
    {
        return libraryLookup;
    }

    public void onValidResendRequest(final long connection, final long correlationId, final boolean slow)
    {
        final FixSenderEndPoint fixSenderEndPoint = connectionIdToSenderEndpoint.get(connection);
        if (fixSenderEndPoint != null)
        {
            fixSenderEndPoint.onValidResendRequest(correlationId, slow);
        }
    }

    public void onStartReplay(
        final long connection, final long correlationId, final long position, final boolean slow)
    {
        final FixSenderEndPoint fixSenderEndPoint = connectionIdToSenderEndpoint.get(connection);
        if (fixSenderEndPoint != null)
        {
            fixSenderEndPoint.onStartReplay(correlationId, position, slow);
        }
    }
}
