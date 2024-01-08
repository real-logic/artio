/*
 * Copyright 2015-2024 Real Logic Limited, Adaptive Financial Consulting Ltd., Monotonic Ltd.
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

import org.agrona.BitUtil;
import org.agrona.ErrorHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Objects;

import static java.nio.channels.SelectionKey.OP_READ;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_LONG;
import static uk.co.real_logic.artio.messages.DisconnectReason.*;

public abstract class ReceiverEndPoint
{
    protected static final int SOCKET_DISCONNECTED = -1;

    protected final GatewayPublication publication;
    protected final TcpChannel channel;
    protected final long connectionId;
    protected boolean hasDisconnected = false;
    protected final MutableAsciiBuffer buffer;
    protected final ByteBuffer byteBuffer;
    protected final ErrorHandler errorHandler;
    protected final Framer framer;

    protected int libraryId;
    protected int usedBufferData = 0;
    protected SelectionKey selectionKey;
    protected AcceptorLogonResult pendingAcceptorLogon;

    private long throttleWindowInNs;
    private int throttleLimitOfMessages;
    private long[] lastMessageTimestampsInNs;
    private int lastMessageTimestampsInNsMask;
    private int throttlePosition;

    public ReceiverEndPoint(
        final GatewayPublication publication,
        final TcpChannel channel,
        final long connectionId,
        final int bufferSize,
        final ErrorHandler errorHandler,
        final Framer framer,
        final int libraryId,
        final int throttleWindowInMs,
        final int throttleLimitOfMessages)
    {
        Objects.requireNonNull(publication, "publication");

        this.publication = publication;
        this.channel = channel;
        this.connectionId = connectionId;
        this.errorHandler = errorHandler;
        this.framer = framer;
        this.libraryId = libraryId;

        byteBuffer = ByteBuffer.allocateDirect(bufferSize);
        buffer = new MutableAsciiBuffer(byteBuffer);

        configureThrottle(throttleWindowInMs, throttleLimitOfMessages);
    }

    void configureThrottle(final int throttleWindowInMs, final int throttleLimitOfMessages)
    {
        if (this.throttleWindowInNs == throttleWindowInMs && this.throttleLimitOfMessages == throttleLimitOfMessages)
        {
            return;
        }

        final long[] oldLastMessageTimestampsInNs = this.lastMessageTimestampsInNs;
        final int oldThrottleLimitOfMessages = this.throttleLimitOfMessages;
        final int oldLastMessageTimestampsInNsMask = this.lastMessageTimestampsInNsMask;
        final int oldThrottlePosition = this.throttlePosition;

        if (throttleWindowInMs == MISSING_INT)
        {
            this.throttleWindowInNs = MISSING_LONG;
            lastMessageTimestampsInNs = null;
            lastMessageTimestampsInNsMask = 0;
        }
        else
        {
            this.throttleWindowInNs = MILLISECONDS.toNanos(throttleWindowInMs);
            final int lastMessageTimestampsInNsCapacity = BitUtil.findNextPositivePowerOfTwo(throttleLimitOfMessages);
            lastMessageTimestampsInNs = new long[lastMessageTimestampsInNsCapacity];
            lastMessageTimestampsInNsMask = lastMessageTimestampsInNsCapacity - 1;
        }
        this.throttleLimitOfMessages = throttleLimitOfMessages;
        throttlePosition = 0;

        if (oldLastMessageTimestampsInNs != null && lastMessageTimestampsInNs != null)
        {
            final int minLimitOfMessages = Math.min(oldThrottleLimitOfMessages, throttleLimitOfMessages);
            int srcPosition = Math.max(0, oldThrottlePosition - minLimitOfMessages);
            while (srcPosition < oldThrottlePosition)
            {
                lastMessageTimestampsInNs[throttlePosition & lastMessageTimestampsInNsMask] =
                    oldLastMessageTimestampsInNs[srcPosition & oldLastMessageTimestampsInNsMask];
                srcPosition++;
                throttlePosition++;
            }
        }
    }

    final boolean shouldThrottle(final long readTimestampInNs)
    {
        final long throttleWindowInNs = this.throttleWindowInNs;
        if (throttleWindowInNs == MISSING_LONG)
        {
            return false;
        }

        final long[] lastMessageTimestampsInNs = this.lastMessageTimestampsInNs;
        final int lastMessageTimestampsMask = this.lastMessageTimestampsInNsMask;
        final int throttlePosition = this.throttlePosition;

        final int oldestMessagePosition = throttlePosition - throttleLimitOfMessages;
        final int oldestMessageIndex = oldestMessagePosition & lastMessageTimestampsMask;
        final long oldestMessageTimestampInNs = lastMessageTimestampsInNs[oldestMessageIndex];

        final int currentIndex = throttlePosition & lastMessageTimestampsMask;
        lastMessageTimestampsInNs[currentIndex] = readTimestampInNs;
        this.throttlePosition = throttlePosition + 1;

        final long timeAgoOfOldestMessageInNs = readTimestampInNs - oldestMessageTimestampInNs;
        return timeAgoOfOldestMessageInNs < throttleWindowInNs;
    }

    long connectionId()
    {
        return connectionId;
    }

    void register(final Selector selector) throws IOException
    {
        selectionKey = channel.register(selector, OP_READ, this);
    }

    void onDisconnectDetected()
    {
        completeDisconnect(REMOTE_DISCONNECT);
    }

    void close(final DisconnectReason reason)
    {
        closeResources();

        if (!hasDisconnected)
        {
            disconnectEndpoint(reason);
        }
    }

    void onNoLogonDisconnect()
    {
        completeDisconnect(NO_LOGON);
    }

    void onAuthenticationTimeoutDisconnect()
    {
        completeDisconnect(AUTHENTICATION_TIMEOUT);
    }

    void completeDisconnect(final DisconnectReason reason)
    {
        disconnectEndpoint(reason);
        removeEndpointFromFramer();
    }

    abstract void removeEndpointFromFramer();

    abstract boolean sendRejectedPendingLogon();

    void disconnectEndpoint(final DisconnectReason reason)
    {
        Objects.requireNonNull(reason);
        framer.schedule(() -> publication.saveDisconnect(libraryId, connectionId, reason));
        cleanupDisconnectState(reason);
        if (selectionKey != null)
        {
            selectionKey.cancel();
        }
        hasDisconnected = true;
    }

    abstract void cleanupDisconnectState(DisconnectReason reason);

    abstract int poll();

    abstract boolean retryFrameMessages();

    abstract boolean requiresAuthentication();

    abstract void closeResources();

    public void libraryId(final int libraryId)
    {
        this.libraryId = libraryId;
    }

    public int libraryId()
    {
        return libraryId;
    }

    boolean hasDisconnected()
    {
        return hasDisconnected;
    }
}
