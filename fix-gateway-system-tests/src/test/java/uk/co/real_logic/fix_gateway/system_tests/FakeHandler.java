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
import java.util.Optional;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.junit.Assert.assertNotEquals;

public class FakeHandler
    implements SessionHandler, SessionAcquireHandler, SessionExistsHandler, SentPositionHandler
{
    private final Long2ObjectHashMap<Session> connectionIdToSession = new Long2ObjectHashMap<>();
    private final OtfParser parser;
    private final FakeOtfAcceptor acceptor;

    private final Deque<CompleteSessionId> completeSessionIds = new ArrayDeque<>();

    private Session lastSession;
    private boolean hasDisconnected = false;
    private long sentPosition;

    public FakeHandler(final FakeOtfAcceptor acceptor)
    {
        this.acceptor = acceptor;
        parser = new OtfParser(acceptor, new IntDictionary());
    }

    // ----------- EVENTS -----------

    public Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final long sessionId,
        final int messageType,
        final long timestampInNs,
        final long position)
    {
        parser.onMessage(buffer, offset, length);
        acceptor.forSession(connectionIdToSession.get(sessionId));
        return CONTINUE;
    }

    public void onTimeout(final int libraryId, final long sessionId)
    {
    }

    public Action onDisconnect(final int libraryId, final long sessionId, final DisconnectReason reason)
    {
        connectionIdToSession.remove(sessionId);
        hasDisconnected = true;
        return CONTINUE;
    }

    public SessionHandler onSessionAcquired(final Session session)
    {
        assertNotEquals(Session.UNKNOWN, session.id());
        connectionIdToSession.put(session.id(), session);
        this.lastSession = session;
        return this;
    }

    public Action onSendCompleted(final long position)
    {
        this.sentPosition = position;
        return CONTINUE;
    }

    public void onSessionExists(
        final FixLibrary library,
        final long sessionId,
        final String acceptorCompId,
        final String acceptorSubId,
        final String acceptorLocationId,
        final String initiatorCompId,
        final String username,
        final String password)
    {
        completeSessionIds.add(new CompleteSessionId(acceptorCompId, initiatorCompId, sessionId));
    }

    // ----------- END EVENTS -----------

    public void resetSession()
    {
        lastSession = null;
    }

    public Collection<Session> sessions()
    {
        return connectionIdToSession.values();
    }

    public boolean hasDisconnected()
    {
        return hasDisconnected;
    }

    public long awaitSessionId(final Runnable poller)
    {
        while (!hasSeenSession())
        {
            poller.run();
            Thread.yield();
        }

        return lastSessionId().sessionId();
    }

    public boolean hasSeenSession()
    {
        return !completeSessionIds.isEmpty();
    }

    public void clearSessions()
    {
        completeSessionIds.clear();
    }

    public long sentPosition()
    {
        return sentPosition;
    }

    public long awaitSessionIdFor(
        final String initiatorId,
        final String acceptorId,
        final Runnable poller)
    {
        while (true)
        {
            poller.run();

            final Optional<CompleteSessionId> maybeSession = completeSessionIds
                .stream()
                .filter((sid) ->
                    sid.initiatorCompId().equals(initiatorId) && sid.acceptorCompId().equals(acceptorId))
                .findFirst();

            if (maybeSession.isPresent())
            {
                return maybeSession.get().sessionId();
            }

            Thread.yield();
        }
    }

    public String lastAcceptorCompId()
    {
        return lastSessionId().acceptorCompId();
    }

    public String lastInitiatorCompId()
    {
        return lastSessionId().initiatorCompId();
    }

    public Session lastSession()
    {
        return lastSession;
    }

    private CompleteSessionId lastSessionId()
    {
        return completeSessionIds.peekFirst();
    }

    public static final class CompleteSessionId
    {
        private final String acceptorCompId;
        private final String initiatorCompId;
        private final long sessionId;

        private CompleteSessionId(final String acceptorCompId, final String initiatorCompId, final long sessionId)
        {
            this.acceptorCompId = acceptorCompId;
            this.initiatorCompId = initiatorCompId;
            this.sessionId = sessionId;
        }

        public String acceptorCompId()
        {
            return acceptorCompId;
        }

        public String initiatorCompId()
        {
            return initiatorCompId;
        }

        public long sessionId()
        {
            return sessionId;
        }
    }
}
