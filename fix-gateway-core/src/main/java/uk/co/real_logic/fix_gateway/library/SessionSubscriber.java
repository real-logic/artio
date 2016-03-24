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
package uk.co.real_logic.fix_gateway.library;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.Timer;
import uk.co.real_logic.fix_gateway.session.AcceptorSession;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.session.SessionParser;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.session.CompositeKey;

import static uk.co.real_logic.fix_gateway.CommonConfiguration.TIME_MESSAGES;

public class SessionSubscriber implements AutoCloseable
{
    private final SessionParser parser;
    private final Session session;
    private final SessionHandler handler;
    private final Timer receiveTimer;
    private final Timer sessionTimer;

    public SessionSubscriber(
        final SessionParser parser,
        final Session session,
        final SessionHandler handler,
        final Timer receiveTimer,
        final Timer sessionTimer)
    {
        this.parser = parser;
        this.session = session;
        this.handler = handler;
        this.receiveTimer = receiveTimer;
        this.sessionTimer = sessionTimer;
    }

    public void onMessage(final DirectBuffer buffer,
                          final int offset,
                          final int length,
                          final int libraryId,
                          final long connectionId,
                          final long sessionId,
                          final int messageType,
                          final long timestamp)
    {
        long now = 0;
        if (TIME_MESSAGES)
        {
            now = receiveTimer.recordSince(timestamp);
        }

        if (parser.onMessage(buffer, offset, length, messageType, sessionId))
        {
            handler.onMessage(buffer, offset, length, libraryId, connectionId, sessionId, messageType, timestamp);
        }

        if (TIME_MESSAGES)
        {
            sessionTimer.recordSince(now);
        }
    }

    public void onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        session.onDisconnect();
        handler.onDisconnect(libraryId, connectionId, reason);
    }

    public void onLogon(
        final long sessionId,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final CompositeKey compositeKey,
        final String username,
        final String password)
    {
        session.id(sessionId);
        if (compositeKey != null)
        {
            session.sessionKey(compositeKey);
        }

        // Acceptors need to wait for Logon message to identify
        if (session instanceof AcceptorSession)
        {
            session.lastSentMsgSeqNum(lastSentSequenceNumber - 1);
            session.lastReceivedMsgSeqNum(lastReceivedSequenceNumber - 1);
        }

        session.username(username);
        session.password(password);
    }

    public int poll(final long time)
    {
        return session.poll(time);
    }

    public void close()
    {
        session.requestDisconnect();
    }

    public Session session()
    {
        return session;
    }
}
