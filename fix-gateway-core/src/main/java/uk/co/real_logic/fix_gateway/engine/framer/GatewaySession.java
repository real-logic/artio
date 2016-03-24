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

import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.session.SessionParser;
import uk.co.real_logic.fix_gateway.messages.ConnectionType;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

class GatewaySession implements SessionInfo
{
    private final long connectionId;
    private final String address;
    private final ConnectionType connectionType;

    private long sessionId;
    private SessionParser sessionParser;
    private Session session;
    private CompositeKey compositeKey;
    private String username;
    private String password;

    GatewaySession(final long connectionId,
                   final long sessionId,
                   final String address,
                   final ConnectionType connectionType,
                   final CompositeKey compositeKey)
    {
        this.connectionId = connectionId;
        this.sessionId = sessionId;
        this.address = address;
        this.connectionType = connectionType;
        this.compositeKey = compositeKey;
    }

    public long connectionId()
    {
        return connectionId;
    }

    public String address()
    {
        return address;
    }

    public long sessionId()
    {
        return sessionId;
    }

    long sendingTimeWindow()
    {
        return 0;
    }

    void manage(final SessionParser sessionParser, final Session session)
    {
        this.sessionParser = sessionParser;
        this.session = session;
    }

    void stopManaging()
    {
        manage(null, null);
    }

    int poll(final long time)
    {
        return session.poll(time);
    }

    Session session()
    {
        return session;
    }

    ConnectionType connectionType()
    {
        return connectionType;
    }

    CompositeKey compositeKey()
    {
        return compositeKey;
    }

    public void onMessage(final MutableAsciiBuffer buffer,
                          final int offset,
                          final int length,
                          final int messageType,
                          final long sessionId)
    {
        if (sessionParser != null)
        {
            sessionParser.onMessage(buffer, offset, length, messageType, sessionId);
        }
    }

    public void onLogon(
        final long sessionId,
        final CompositeKey compositeKey,
        final String username,
        final String password)
    {
        this.sessionId = sessionId;
        this.compositeKey = compositeKey;
        this.username = username;
        this.password = password;
    }

    public String username()
    {
        return username;
    }

    public String password()
    {
        return password;
    }
}
