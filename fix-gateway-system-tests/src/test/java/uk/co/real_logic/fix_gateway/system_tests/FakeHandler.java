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

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.library.*;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.otf.OtfParser;
import uk.co.real_logic.fix_gateway.session.Session;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;

import static org.junit.Assert.assertNotEquals;

public class FakeHandler implements SessionHandler, SessionAcquireHandler, SessionExistsHandler, SentPositionHandler
{

    private final Long2ObjectHashMap<Session> connectionIdToSession = new Long2ObjectHashMap<>();
    private final OtfParser parser;
    private final FakeOtfAcceptor acceptor;
    private final Deque<Long> sessionIds = new ArrayDeque<>();

    private Session latestSession;
    private boolean hasDisconnected = false;
    private long sentPosition;
    private String lastAcceptorCompId;
    private String lastInitiatorCompId;

    public FakeHandler(final FakeOtfAcceptor acceptor)
    {
        this.acceptor = acceptor;
        parser = new OtfParser(acceptor, new IntDictionary());
    }

    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final long sessionId,
        final int messageType,
        final long timestamp,
        final long position)
    {
        parser.onMessage(buffer, offset, length);
        acceptor.forSession(connectionIdToSession.get(sessionId));
        return Action.CONTINUE;
    }

    public Action onDisconnect(final int libraryId, final long sessionId, final DisconnectReason reason)
    {
        connectionIdToSession.remove(sessionId);
        hasDisconnected = true;
        return Action.CONTINUE;
    }

    public SessionHandler onSessionAcquired(final Session session)
    {
        assertNotEquals(Session.UNKNOWN, session.id());
        connectionIdToSession.put(session.id(), session);
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

    public long latestSessionId()
    {
        return sessionIds.peekFirst();
    }

    public boolean hasSession()
    {
        return !sessionIds.isEmpty();
    }

    public void clearConnections()
    {
        sessionIds.clear();
    }

    public Action onSendCompleted(final long position)
    {
        this.sentPosition = position;
        return Action.CONTINUE;
    }

    public long sentPosition()
    {
        return sentPosition;
    }

    public void onSessionExists(final FixLibrary library,
                                final long sessionId,
                                final String acceptorCompId,
                                final String acceptorSubId,
                                final String acceptorLocationId,
                                final String initiatorCompId,
                                final String username,
                                final String password)
    {
        this.lastAcceptorCompId = acceptorCompId;
        this.lastInitiatorCompId = initiatorCompId;
        sessionIds.add(sessionId);
    }

    public String lastAcceptorCompId()
    {
        return lastAcceptorCompId;
    }

    public String lastInitiatorCompId()
    {
        return lastInitiatorCompId;
    }
}
