/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.library;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.MessageStatus;
import uk.co.real_logic.artio.session.*;
import uk.co.real_logic.artio.timing.Timer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static uk.co.real_logic.artio.messages.GatewayError.UNABLE_TO_LOGON;

class SessionSubscriber implements AutoCloseable
{
    private final SessionParser parser;
    private final InternalSession session;
    private final Timer receiveTimer;
    private final Timer sessionTimer;

    private SessionHandler handler;
    private InitiateSessionReply initiateSessionReply;

    SessionSubscriber(
        final SessionParser parser,
        final InternalSession session,
        final Timer receiveTimer,
        final Timer sessionTimer)
    {
        this.parser = parser;
        this.session = session;
        this.receiveTimer = receiveTimer;
        this.sessionTimer = sessionTimer;
        this.session.logonListener(this::onSessionLogon);
    }

    Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final long sessionId,
        final int sequenceIndex,
        final int messageType,
        final long timestamp,
        final MessageStatus status,
        final long position)
    {
        final long now = receiveTimer.recordSince(timestamp);

        try
        {
            switch (status)
            {
                case OK:
                    final Action action = parser.onMessage(buffer, offset, length, messageType, sessionId);
                    if (action == BREAK)
                    {
                        return BREAK;
                    }

                    // Can receive messages when no longer disconnected.
                    final Action handlerAction = handler.onMessage(
                        buffer,
                        offset,
                        length,
                        libraryId,
                        session,
                        sequenceIndex,
                        messageType,
                        timestamp,
                        position);

                    if (handlerAction == CONTINUE || handlerAction == COMMIT)
                    {
                        session.updateLastMessageProcessed();
                    }

                    return handlerAction;

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
                        position);

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

    void onLogon(
        final long sessionId,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final CompositeKey compositeKey)
    {
        if (compositeKey != null)
        {
            session.setupSession(sessionId, compositeKey);
        }
        else
        {
            session.id(sessionId);
        }

        // Acceptors need to wait for Logon message to identify
        if (session instanceof AcceptorSession)
        {
            session.lastSentMsgSeqNum(lastSentSequenceNumber);
            session.lastReceivedMsgSeqNum(lastReceivedSequenceNumber);
        }
    }

    private void onSessionLogon(final Session session)
    {
        // Should only be fired if we already own the session and the client sends another logon to run and end of day.
        if (session.logonTime() != Session.NO_LOGON_TIME)
        {
            handler.onSessionStart(session);
        }
        if (initiateSessionReply != null)
        {
            initiateSessionReply.onComplete(session);
            // Don't want to hold a reference to the reply object for the
            // lifetime of the Session
            initiateSessionReply = null;
        }
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
