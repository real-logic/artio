/*
 * Copyright 2015-2020 Real Logic Limited., Monotonic Ltd.
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
package uk.co.real_logic.artio;


import org.agrona.AsciiSequenceView;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.AbstractDebugAppender.ThreadLocalAppender;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.util.CharFormatter;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.ServiceLoader;

import static uk.co.real_logic.artio.CommonConfiguration.*;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEBUG_PRINT_MESSAGES;

/**
 * A logger purely for debug data. Not optimised for high performance logging, but all logging calls must be removable
 * by the optimiser.
 */
public final class DebugLogger
{
    private static final AbstractDebugAppender APPENDER;
    private static final ThreadLocal<ThreadLocalLogger> THREAD_LOCAL = ThreadLocal.withInitial(ThreadLocalLogger::new);

    static
    {
        final ServiceLoader<AbstractDebugAppender> loader = ServiceLoader.load(AbstractDebugAppender.class);
        final Iterator<AbstractDebugAppender> it = loader.iterator();
        if (it.hasNext())
        {
            APPENDER = it.next();
        }
        else
        {
            APPENDER = new PrintingDebugAppender();
        }
    }

    public static void log(
        final LogTag tag,
        final CharFormatter formatter)
    {
        THREAD_LOCAL.get().log(tag, formatter);
    }

    public static void log(
        final LogTag tag,
        final CharFormatter formatter,
        final int value,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (isEnabled(tag))
        {
            formatter.clear().with(value);
            THREAD_LOCAL.get().log(tag, formatter, buffer, offset, length);
        }
    }

    public static void log(
        final LogTag tag,
        final CharFormatter formatter,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().log(tag, formatter, buffer, offset, length);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final RedactSequenceUpdateEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ManageSessionEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final DisconnectEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ConnectEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ResetSessionIdsEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ResetSequenceNumberEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ResetLibrarySequenceNumberEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final RequestDisconnectEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final MidConnectionDisconnectEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final InitiateConnectionEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ErrorEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ApplicationHeartbeatEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final LibraryConnectEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ReleaseSessionEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ReleaseSessionReplyEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final RequestSessionEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final RequestSessionReplyEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final NewSentPositionEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final LibraryTimeoutEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ControlNotificationEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final SlowStatusNotificationEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final FollowerSessionRequestEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final FollowerSessionReplyEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final EndOfDayEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final WriteMetaDataEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final WriteMetaDataReplyEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ReadMetaDataEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ReadMetaDataReplyEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ReplayMessagesEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag,
        final ReplayMessagesReplyEncoder encoder)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().logSbeMessage(tag, encoder);
        }
    }

    public static void logSbeMessage(
        final LogTag tag, final InitiateILinkConnectionEncoder encoder)
    {
        // TODO
    }

    public static void logSbeMessage(
        final LogTag tag, final ILinkConnectEncoder encoder)
    {
        // TODO
    }

    public static void log(
        final LogTag tag,
        final String prefixString,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().log(tag, prefixString, buffer, offset, length);
        }
    }

    public static void log(
        final LogTag tag,
        final String prefixString,
        final ByteBuffer byteBuffer,
        final int length)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().log(tag, prefixString, byteBuffer, length);
        }
    }

    public static void log(
        final LogTag tag,
        final String message)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().log(tag, message);
        }
    }

    public static void log(
        final LogTag tag,
        final String prefixString,
        final String suffixString)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().log(tag, prefixString, suffixString);
        }
    }

    public static void log(
        final LogTag tag,
        final CharFormatter formatter,
        final long first,
        final String second)
    {
        if (isEnabled(tag))
        {
            formatter.clear().with(first).with(second);
            THREAD_LOCAL.get().log(tag, formatter);
        }
    }

    public static void log(
        final LogTag tag,
        final CharFormatter formatter,
        final long first,
        final long second)
    {
        if (isEnabled(tag))
        {
            formatter.clear().with(first).with(second);
            THREAD_LOCAL.get().log(tag, formatter);
        }
    }

    public static void log(
        final LogTag tag,
        final CharFormatter formatter,
        final long first,
        final long second,
        final long third)
    {
        if (isEnabled(tag))
        {
            formatter.clear().with(first).with(second).with(third);
            THREAD_LOCAL.get().log(tag, formatter);
        }
    }

    public static void log(
        final LogTag tag,
        final CharFormatter formatter,
        final String first,
        final long second,
        final long third)
    {
        if (isEnabled(tag))
        {
            formatter.clear().with(first).with(second).with(third);
            THREAD_LOCAL.get().log(tag, formatter);
        }
    }

    public static void log(
        final LogTag tag,
        final CharFormatter formatter,
        final long first,
        final long second,
        final String third)
    {
        if (isEnabled(tag))
        {
            formatter.clear().with(first).with(second).with(third);
            THREAD_LOCAL.get().log(tag, formatter);
        }
    }

    public static void log(
        final LogTag tag,
        final CharFormatter formatter,
        final long first,
        final long second,
        final long third,
        final long fourth)
    {
        if (isEnabled(tag))
        {
            formatter.clear().with(first).with(second).with(third).with(fourth);
            THREAD_LOCAL.get().log(tag, formatter);
        }
    }

    public static void log(
        final LogTag tag,
        final CharFormatter formatter,
        final String first,
        final long second,
        final long third,
        final long fourth)
    {
        if (isEnabled(tag))
        {
            formatter.clear().with(first).with(second).with(third).with(fourth);
            THREAD_LOCAL.get().log(tag, formatter);
        }
    }

    // Used by fix-integration project
    public static void log(
        final LogTag tag,
        final String formatString,
        final Object first,
        final Object second)
    {
        if (isEnabled(tag))
        {
            THREAD_LOCAL.get().log(tag, String.format(formatString, first, second));
        }
    }

    private static String threadName()
    {
        return Thread.currentThread().getName();
    }

    private static void substituteSeparator(final byte[] data)
    {
        if (needsSeparatorSubstitution())
        {
            final int size = data.length;
            for (int i = 0; i < size; i++)
            {
                if (data[i] == DEFAULT_DEBUG_LOGGING_SEPARATOR)
                {
                    data[i] = DEBUG_LOGGING_SEPARATOR;
                }
            }
        }
    }

    private static boolean needsSeparatorSubstitution()
    {
        return DEBUG_LOGGING_SEPARATOR != DEFAULT_DEBUG_LOGGING_SEPARATOR;
    }

    public static boolean isEnabled(final LogTag tag)
    {
        return DEBUG_PRINT_MESSAGES && DEBUG_TAGS.contains(tag);
    }

    static class ThreadLocalLogger
    {
        // Library -> Engine
        private final InitiateConnectionDecoder initiateConnection = new InitiateConnectionDecoder();
        private final RequestDisconnectDecoder requestDisconnect = new RequestDisconnectDecoder();
        private final MidConnectionDisconnectDecoder midConnectionDisconnect = new MidConnectionDisconnectDecoder();
        private final LibraryConnectDecoder libraryConnect = new LibraryConnectDecoder();
        private final ReleaseSessionDecoder releaseSession = new ReleaseSessionDecoder();
        private final RequestSessionDecoder requestSession = new RequestSessionDecoder();
        private final FollowerSessionRequestDecoder followerSessionRequest = new FollowerSessionRequestDecoder();
        private final WriteMetaDataDecoder writeMetaData = new WriteMetaDataDecoder();
        private final ReadMetaDataDecoder readMetaData = new ReadMetaDataDecoder();
        private final ReplayMessagesDecoder replayMessages = new ReplayMessagesDecoder();
        private final ConnectDecoder connect = new ConnectDecoder();
        private final ResetSessionIdsDecoder resetSessionIds = new ResetSessionIdsDecoder();
        private final LibraryTimeoutDecoder libraryTimeout = new LibraryTimeoutDecoder();

        // Engine -> Library
        private final ErrorDecoder error = new ErrorDecoder();
        private final ReleaseSessionReplyDecoder releaseSessionReply = new ReleaseSessionReplyDecoder();
        private final RequestSessionReplyDecoder requestSessionReply = new RequestSessionReplyDecoder();
        private final WriteMetaDataReplyDecoder writeMetaDataReply = new WriteMetaDataReplyDecoder();
        private final ReadMetaDataReplyDecoder readMetaDataReply = new ReadMetaDataReplyDecoder();
        private final NewSentPositionDecoder newSentPosition = new NewSentPositionDecoder();
        private final ControlNotificationDecoder controlNotification = new ControlNotificationDecoder();
        private final SlowStatusNotificationDecoder slowStatusNotification = new SlowStatusNotificationDecoder();
        private final ResetLibrarySequenceNumberDecoder resetLibrarySequenceNumber =
            new ResetLibrarySequenceNumberDecoder();
        private final ResetSequenceNumberDecoder resetSequenceNumber =
            new ResetSequenceNumberDecoder();
        private final ManageSessionDecoder manageSession = new ManageSessionDecoder();
        private final FollowerSessionReplyDecoder followerSessionReply = new FollowerSessionReplyDecoder();
        private final EndOfDayDecoder endOfDay = new EndOfDayDecoder();
        private final ReplayMessagesReplyDecoder replayMessagesReply = new ReplayMessagesReplyDecoder();

        // Common
        private final ApplicationHeartbeatDecoder applicationHeartbeat = new ApplicationHeartbeatDecoder();
        private final DisconnectDecoder disconnect = new DisconnectDecoder();
        private final RedactSequenceUpdateDecoder redactSequenceUpdate = new RedactSequenceUpdateDecoder();

        private final StringBuilder builder = new StringBuilder();
        private final char[] threadName;

        private byte[] bytes = new byte[0];
        private final AsciiSequenceView asciiView = new AsciiSequenceView();
        private final UnsafeBuffer buffer = new UnsafeBuffer(bytes);
        private final ThreadLocalAppender appender;

        final boolean isThreadEnabled;

        ThreadLocalLogger()
        {
            final String threadName = threadName();
            isThreadEnabled = DEBUG_PRINT_THREAD == null || DEBUG_PRINT_THREAD.equals(threadName);
            this.threadName = (":" + threadName).toCharArray();
            appender = APPENDER.makeLocalAppender();
        }

        public void logSbeMessage(final LogTag tag, final RedactSequenceUpdateEncoder encoder)
        {
            appendStart(tag);
            redactSequenceUpdate.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                RedactSequenceUpdateEncoder.BLOCK_LENGTH,
                RedactSequenceUpdateEncoder.SCHEMA_VERSION);
            redactSequenceUpdate.appendTo(builder);
            finish();
        }

        public void logSbeMessage(final LogTag tag, final ManageSessionEncoder encoder)
        {
            appendStart(tag);
            manageSession.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ManageSessionEncoder.BLOCK_LENGTH,
                ManageSessionEncoder.SCHEMA_VERSION);
            manageSession.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final DisconnectEncoder encoder)
        {
            appendStart(tag);
            disconnect.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                DisconnectEncoder.BLOCK_LENGTH,
                DisconnectEncoder.SCHEMA_VERSION);
            disconnect.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ConnectEncoder encoder)
        {
            appendStart(tag);
            connect.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ConnectEncoder.BLOCK_LENGTH,
                ConnectEncoder.SCHEMA_VERSION);
            connect.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ResetSessionIdsEncoder encoder)
        {
            appendStart(tag);
            resetSessionIds.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ResetSessionIdsEncoder.BLOCK_LENGTH,
                ResetSessionIdsEncoder.SCHEMA_VERSION);
            resetSessionIds.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ResetSequenceNumberEncoder encoder)
        {
            appendStart(tag);
            resetSequenceNumber.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ResetSequenceNumberEncoder.BLOCK_LENGTH,
                ResetSequenceNumberEncoder.SCHEMA_VERSION);
            resetSequenceNumber.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ResetLibrarySequenceNumberEncoder encoder)
        {
            appendStart(tag);
            resetLibrarySequenceNumber.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ResetLibrarySequenceNumberEncoder.BLOCK_LENGTH,
                ResetLibrarySequenceNumberEncoder.SCHEMA_VERSION);
            resetLibrarySequenceNumber.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final RequestDisconnectEncoder encoder)
        {
            appendStart(tag);
            requestDisconnect.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                RequestDisconnectEncoder.BLOCK_LENGTH,
                RequestDisconnectEncoder.SCHEMA_VERSION);
            requestDisconnect.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final MidConnectionDisconnectEncoder encoder)
        {
            appendStart(tag);
            midConnectionDisconnect.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                MidConnectionDisconnectEncoder.BLOCK_LENGTH,
                MidConnectionDisconnectEncoder.SCHEMA_VERSION);
            midConnectionDisconnect.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final InitiateConnectionEncoder encoder)
        {
            appendStart(tag);
            initiateConnection.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                InitiateConnectionEncoder.BLOCK_LENGTH,
                InitiateConnectionEncoder.SCHEMA_VERSION);
            initiateConnection.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ErrorEncoder encoder)
        {
            appendStart(tag);
            error.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ErrorEncoder.BLOCK_LENGTH,
                ErrorEncoder.SCHEMA_VERSION);
            error.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ApplicationHeartbeatEncoder encoder)
        {
            appendStart(tag);
            applicationHeartbeat.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ApplicationHeartbeatEncoder.BLOCK_LENGTH,
                ApplicationHeartbeatEncoder.SCHEMA_VERSION);
            applicationHeartbeat.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final LibraryConnectEncoder encoder)
        {
            appendStart(tag);
            libraryConnect.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                LibraryConnectEncoder.BLOCK_LENGTH,
                LibraryConnectEncoder.SCHEMA_VERSION);
            libraryConnect.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ReleaseSessionEncoder encoder)
        {
            appendStart(tag);
            releaseSession.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ReleaseSessionEncoder.BLOCK_LENGTH,
                ReleaseSessionEncoder.SCHEMA_VERSION);
            releaseSession.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ReleaseSessionReplyEncoder encoder)
        {
            appendStart(tag);
            releaseSessionReply.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ReleaseSessionReplyEncoder.BLOCK_LENGTH,
                ReleaseSessionReplyEncoder.SCHEMA_VERSION);
            releaseSessionReply.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final RequestSessionEncoder encoder)
        {
            appendStart(tag);
            requestSession.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                RequestSessionEncoder.BLOCK_LENGTH,
                RequestSessionEncoder.SCHEMA_VERSION);
            requestSession.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final RequestSessionReplyEncoder encoder)
        {
            appendStart(tag);
            requestSessionReply.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                RequestSessionReplyEncoder.BLOCK_LENGTH,
                RequestSessionReplyEncoder.SCHEMA_VERSION);
            requestSessionReply.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final NewSentPositionEncoder encoder)
        {
            appendStart(tag);
            newSentPosition.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                NewSentPositionEncoder.BLOCK_LENGTH,
                NewSentPositionEncoder.SCHEMA_VERSION);
            newSentPosition.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final LibraryTimeoutEncoder encoder)
        {
            appendStart(tag);
            libraryTimeout.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                LibraryTimeoutEncoder.BLOCK_LENGTH,
                LibraryTimeoutEncoder.SCHEMA_VERSION);
            libraryTimeout.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ControlNotificationEncoder encoder)
        {
            appendStart(tag);
            controlNotification.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ControlNotificationEncoder.BLOCK_LENGTH,
                ControlNotificationEncoder.SCHEMA_VERSION);
            controlNotification.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final SlowStatusNotificationEncoder encoder)
        {
            appendStart(tag);
            slowStatusNotification.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                SlowStatusNotificationEncoder.BLOCK_LENGTH,
                SlowStatusNotificationEncoder.SCHEMA_VERSION);
            slowStatusNotification.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final FollowerSessionRequestEncoder encoder)
        {
            appendStart(tag);
            followerSessionRequest.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                FollowerSessionRequestEncoder.BLOCK_LENGTH,
                FollowerSessionRequestEncoder.SCHEMA_VERSION);
            followerSessionRequest.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final FollowerSessionReplyEncoder encoder)
        {
            appendStart(tag);
            followerSessionReply.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                FollowerSessionReplyEncoder.BLOCK_LENGTH,
                FollowerSessionReplyEncoder.SCHEMA_VERSION);
            followerSessionReply.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final EndOfDayEncoder encoder)
        {
            appendStart(tag);
            endOfDay.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                EndOfDayEncoder.BLOCK_LENGTH,
                EndOfDayEncoder.SCHEMA_VERSION);
            endOfDay.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final WriteMetaDataEncoder encoder)
        {
            appendStart(tag);
            writeMetaData.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                WriteMetaDataEncoder.BLOCK_LENGTH,
                WriteMetaDataEncoder.SCHEMA_VERSION);
            writeMetaData.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final WriteMetaDataReplyEncoder encoder)
        {
            appendStart(tag);
            writeMetaDataReply.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                WriteMetaDataReplyEncoder.BLOCK_LENGTH,
                WriteMetaDataReplyEncoder.SCHEMA_VERSION);
            writeMetaDataReply.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ReadMetaDataEncoder encoder)
        {
            appendStart(tag);
            readMetaData.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ReadMetaDataEncoder.BLOCK_LENGTH,
                ReadMetaDataEncoder.SCHEMA_VERSION);
            readMetaData.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ReadMetaDataReplyEncoder encoder)
        {
            appendStart(tag);
            readMetaDataReply.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ReadMetaDataReplyEncoder.BLOCK_LENGTH,
                ReadMetaDataReplyEncoder.SCHEMA_VERSION);
            readMetaDataReply.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ReplayMessagesEncoder encoder)
        {
            appendStart(tag);
            replayMessages.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ReplayMessagesEncoder.BLOCK_LENGTH,
                ReplayMessagesEncoder.SCHEMA_VERSION);
            replayMessages.appendTo(builder);
            finish();
        }

        public void logSbeMessage(
            final LogTag tag,
            final ReplayMessagesReplyEncoder encoder)
        {
            appendStart(tag);
            replayMessagesReply.wrap(
                encoder.buffer(),
                encoder.initialOffset(),
                ReplayMessagesReplyEncoder.BLOCK_LENGTH,
                ReplayMessagesReplyEncoder.SCHEMA_VERSION);
            replayMessagesReply.appendTo(builder);
            finish();
        }

        private void appendStart(final LogTag tag)
        {
            final StringBuilder builder = this.builder;
            builder.setLength(0);
            builder.append(System.currentTimeMillis());
            builder.append(threadName);
            builder.append(tag.logStr());
        }

        public void log(final LogTag tag, final String prefixString, final ByteBuffer byteBuffer, final int length)
        {
            final byte[] data = getByteArray(length);
            byteBuffer.get(data, 0, length);
            substituteSeparator(data);

            appendStart(tag);
            final StringBuilder builder = this.builder;
            builder.append(prefixString);
            final AsciiSequenceView asciiView = this.asciiView;
            asciiView.wrap(buffer, 0, length);
            builder.append(asciiView);
            finish();
        }

        private byte[] getByteArray(final int length)
        {
            byte[] data = this.bytes;
            if (data.length < length)
            {
                this.bytes = data = new byte[length];
                buffer.wrap(data);
            }
            return data;
        }

        public void log(
            final LogTag tag, final String message)
        {
            appendStart(tag);
            builder.append(message);
            finish();
        }

        public void log(
            final LogTag tag, final String prefixString, final DirectBuffer buffer, final int offset, final int length)
        {
            appendStart(tag);
            builder.append(prefixString);

            if (needsSeparatorSubstitution())
            {
                final byte[] data = getByteArray(length);
                buffer.getBytes(offset, data, 0, length);
                substituteSeparator(data);
                asciiView.wrap(this.buffer, 0, length);
            }
            else
            {
                asciiView.wrap(buffer, offset, length);
            }

            builder.append(asciiView);
            finish();
        }

        public void log(
            final LogTag tag,
            final CharFormatter formatter,
            final DirectBuffer buffer,
            final int bufferOffset,
            final int bufferLength)
        {
            final byte[] data = getByteArray(bufferLength);
            buffer.getBytes(bufferOffset, data, 0, bufferLength);
            substituteSeparator(data);

            formatter.with(data, bufferLength);

            log(tag, formatter);
        }

        public void log(
            final LogTag tag,
            final String prefixString,
            final String suffixString)
        {
            appendStart(tag);
            final StringBuilder builder = this.builder;
            builder.append(prefixString);
            builder.append(suffixString);
        }

        public void log(
            final LogTag tag,
            final CharFormatter formatter)
        {
            appendStart(tag);
            formatter.appendTo(builder);
        }


        private void finish()
        {
            final StringBuilder builder = this.builder;
            builder.append(System.lineSeparator());
            appender.log(builder);
        }
    }
}
