/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.library.session;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.Timer;

import static uk.co.real_logic.fix_gateway.CommonConfiguration.TIME_MESSAGES;

public class SessionSubscriber implements AutoCloseable
{
    private final SessionParser parser;
    private final Session session;
    private final SessionHandler handler;
    private final Timer timer;

    public SessionSubscriber(
        final SessionParser parser,
        final Session session,
        final SessionHandler handler,
        final Timer timer)
    {
        this.parser = parser;
        this.session = session;
        this.handler = handler;
        this.timer = timer;
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
        if (TIME_MESSAGES)
        {
            timer.recordSince(timestamp);
        }

        if (parser.onMessage(buffer, offset, length, messageType, sessionId))
        {
            handler.onMessage(buffer, offset, length, libraryId, connectionId, sessionId, messageType, timestamp);
        }
    }

    public void onDisconnect(final int libraryId, final long connectionId)
    {
        session.onDisconnect();
        handler.onDisconnect(libraryId, connectionId);
    }

    public void onLogon(final long connectionId, final long sessionId)
    {
        session.id(sessionId);
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
