/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.artio.library;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.MessageStatus;
import uk.co.real_logic.artio.messages.ReplayMessagesStatus;
import uk.co.real_logic.artio.session.*;
import uk.co.real_logic.artio.timing.Timer;

import java.util.function.BooleanSupplier;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static uk.co.real_logic.artio.messages.GatewayError.UNABLE_TO_LOGON;

class SessionSubscriber implements AutoCloseable, SessionProcessHandler
{
    private final OnMessageInfo info;
    private final SessionParser parser;
    private final InternalSession session;
    private final Timer receiveTimer;
    private final Timer sessionTimer;
    private final LibraryPoller libraryPoller;

    private SessionHandler handler;
    private InitiateSessionReply initiateSessionReply;
    private boolean userAbortedLastMessage = false;

    SessionSubscriber(
        final OnMessageInfo info,
        final SessionParser parser,
        final InternalSession session,
        final Timer receiveTimer,
        final Timer sessionTimer,
        final LibraryPoller libraryPoller)
    {
        this.info = info;
        this.parser = parser;
        this.session = session;
        this.receiveTimer = receiveTimer;
        this.sessionTimer = sessionTimer;
        this.libraryPoller = libraryPoller;
        this.session.sessionProcessHandler(this);
    }

    Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final int sequenceIndex,
        final long messageType,
        final long timestamp,
        final MessageStatus status,
        final long position)
    {
        final long now = receiveTimer.recordSince(timestamp);

        final OnMessageInfo info = this.info;
        info.status(status);
        // this gets set to false by the Session when a problem is detected.
        info.isValid(true);

        try
        {
            switch (status)
            {
                case OK:
                    final boolean userAbortedLastMessage = this.userAbortedLastMessage;
                    if (userAbortedLastMessage)
                    {
                        // Don't re-run the parser / session handling logic if you're on the retry path
                        final Action handlerAction = handler.onMessage(
                            buffer,
                            offset,
                            length,
                            libraryId,
                            session,
                            sequenceIndex,
                            messageType,
                            timestamp,
                            position,
                            info);

                        if (handlerAction != ABORT)
                        {
                            session.updateLastMessageProcessed();
                            this.userAbortedLastMessage = false;
                        }

                        return handlerAction;
                    }
                    else
                    {
                        final Action action = parser.onMessage(
                            buffer, offset, length, messageType, position);
                        if (action == ABORT)
                        {
                            return ABORT;
                        }

                        final Action handlerAction = handler.onMessage(
                            buffer,
                            offset,
                            length,
                            libraryId,
                            session,
                            sequenceIndex,
                            messageType,
                            timestamp,
                            position,
                            info);

                        if (handlerAction == ABORT)
                        {
                            this.userAbortedLastMessage = true;
                        }
                        else
                        {
                            session.updateLastMessageProcessed();
                        }

                        return handlerAction;
                    }

                case CATCHUP_REPLAY:
                    return handler.onMessage(
                        buffer,
                        offset,
                        length,
                        libraryId,
                        session,
                        sequenceIndex,
                        messageType,
                        timestamp,
                        position,
                        info);

                default:
                    return CONTINUE;
            }
        }
        finally
        {
            sessionTimer.recordSince(now);
        }
    }

    Action onDisconnect(final int libraryId, final DisconnectReason reason)
    {
        final Action action = handler.onDisconnect(libraryId, session, reason);
        if (action != ABORT)
        {
            session.onDisconnect();
            // We've been disconnected before an initiator session has finished logging on, eg: wrong msgSeqNum in logon
            if (initiateSessionReply != null)
            {
                initiateSessionReply.onError(UNABLE_TO_LOGON, "Disconnected before session active");
                initiateSessionReply = null;
            }
        }
        return action;
    }

    public void onLogon(final Session session)
    {
        handler.onSessionStart(session);

        if (initiateSessionReply != null)
        {
            initiateSessionReply.onComplete(session);
            // Don't want to hold a reference to the reply object for the
            // lifetime of the Session
            initiateSessionReply = null;
        }
    }

    public Reply<ReplayMessagesStatus> replayReceivedMessages(
        final long sessionId,
        final int replayFromSequenceNumber,
        final int replayFromSequenceIndex,
        final int replayToSequenceNumber,
        final int replayToSequenceIndex,
        final long timeout)
    {
        return new ReplayMessagesReply(
            libraryPoller,
            libraryPoller.timeInMs() + timeout,
            sessionId,
            replayFromSequenceNumber,
            replayFromSequenceIndex,
            replayToSequenceNumber,
            replayToSequenceIndex);
    }

    public void enqueueTask(final BooleanSupplier task)
    {
        libraryPoller.enqueueTask(task);
    }

    void onTimeout(final int libraryId)
    {
        handler.onTimeout(libraryId, session);
    }

    void onSlowStatusNotification(final int libraryId, final boolean hasBecomeSlow)
    {
        handler.onSlowStatus(libraryId, session, hasBecomeSlow);
    }

    public void close()
    {
        session.requestDisconnect();
    }

    InternalSession session()
    {
        return session;
    }

    void handler(final SessionHandler handler)
    {
        this.handler = handler;
    }

    void reply(final InitiateSessionReply reply)
    {
        this.initiateSessionReply = reply;
    }
}
