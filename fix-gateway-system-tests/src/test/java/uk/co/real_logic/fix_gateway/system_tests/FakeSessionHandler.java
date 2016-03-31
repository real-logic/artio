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
package uk.co.real_logic.fix_gateway.system_tests;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.NewConnectHandler;
import uk.co.real_logic.fix_gateway.library.NewSessionHandler;
import uk.co.real_logic.fix_gateway.library.SessionHandler;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.otf.OtfParser;
import uk.co.real_logic.fix_gateway.session.Session;

import java.util.*;

public class FakeSessionHandler implements SessionHandler, NewSessionHandler, NewConnectHandler
{

    private final Long2ObjectHashMap<Session> connectionIdToSession = new Long2ObjectHashMap<>();
    private final OtfParser parser;
    private final FakeOtfAcceptor acceptor;

    private Session latestSession;
    private long connectionId = -1;
    private boolean hasDisconnected = false;

    public FakeSessionHandler(final FakeOtfAcceptor acceptor)
    {
        this.acceptor = acceptor;
        parser = new OtfParser(acceptor, new IntDictionary());
    }

    public void onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int messageType,
        final long timestamp)
    {
        parser.onMessage(buffer, offset, length);
        acceptor.forSession(connectionIdToSession.get(connectionId));
    }

    public void onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        this.connectionId = connectionId;
        connectionIdToSession.remove(connectionId);
        hasDisconnected = true;
    }

    public long connectionId()
    {
        return connectionId;
    }

    public SessionHandler onConnect(final Session session)
    {
        connectionIdToSession.put(session.connectionId(), session);
        this.latestSession = session;
        return this;
    }

    public Session latestSession()
    {
        return latestSession;
    }

    public void resetSession()
    {
        latestSession = null;
    }

    public Collection<Session> sessions()
    {
        return connectionIdToSession.values();
    }

    public boolean hasDisconnected()
    {
        return hasDisconnected;
    }

    private Deque<Long> connections = new ArrayDeque<>();

    public void onConnect(final FixLibrary library, final long connectionId, final String address)
    {
        connections.addFirst(connectionId);
    }

    public long latestConnection()
    {
        return connections.peekFirst();
    }
}
