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
package uk.co.real_logic.fix_gateway.system_tests;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.Timing;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.library.*;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.otf.OtfParser;
import uk.co.real_logic.fix_gateway.session.Session;

import java.util.*;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.junit.Assert.assertNotEquals;

public class FakeHandler
    implements SessionHandler, SessionAcquireHandler, SessionExistsHandler, SentPositionHandler
{
    private final OtfParser parser;
    private final FakeOtfAcceptor acceptor;

    private final List<Session> sessions = new ArrayList<>();
    private final Set<Session> slowSessions = new HashSet<>();
    private final Deque<CompleteSessionId> completeSessionIds = new ArrayDeque<>();

    private Session lastSession;
    private boolean hasDisconnected = false;
    private long sentPosition;
    private boolean lastSessionWasSlow;

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
        final Session session,
        final int sequenceIndex,
        final int messageType,
        final long timestampInNs,
        final long position)
    {
        parser.onMessage(buffer, offset, length);
        acceptor.lastMessage().sequenceIndex(sequenceIndex);
        acceptor.forSession(session);
        return CONTINUE;
    }

    public void onTimeout(final int libraryId, final Session session)
    {
    }

    public void onSlowStatus(final int libraryId, final Session session, final boolean hasBecomeSlow)
    {
        if (hasBecomeSlow)
        {
            slowSessions.add(session);
        }
        else
        {
            slowSessions.remove(session);
        }
    }

    public Action onDisconnect(final int libraryId, final Session session, final DisconnectReason reason)
    {
        sessions.remove(session);
        hasDisconnected = true;
        return CONTINUE;
    }

    @Override
    public void onSessionStart(final Session session)
    {
    }

    public SessionHandler onSessionAcquired(final Session session, final boolean isSlow)
    {
        assertNotEquals(Session.UNKNOWN, session.id());
        sessions.add(session);
        this.lastSession = session;
        this.lastSessionWasSlow = isSlow;
        return this;
    }

    public Action onSendCompleted(final long position)
    {
        this.sentPosition = position;
        return CONTINUE;
    }

    public void onSessionExists(
        final FixLibrary library,
        final long surrogateId,
        final String localCompId,
        final String localSubId,
        final String localLocationId,
        final String remoteCompId,
        final String remoteSubId,
        final String remoteLocationId)
    {
        completeSessionIds.add(new CompleteSessionId(localCompId, remoteCompId, surrogateId));
    }

    // ----------- END EVENTS -----------

    public void resetSession()
    {
        lastSession = null;
    }

    public List<Session> sessions()
    {
        return sessions;
    }

    public boolean hasDisconnected()
    {
        return hasDisconnected;
    }

    public long awaitSessionId(final Runnable poller)
    {
        return awaitCompleteSessionId(poller).surrogateId();
    }

    public CompleteSessionId awaitCompleteSessionId(final Runnable poller)
    {
        Timing.assertEventuallyTrue(
            "Couldn't find session Id",
            () ->
            {
                poller.run();
                return hasSeenSession();
            });

        return lastSessionId();
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

    long awaitSessionIdFor(
        final String initiatorId,
        final String acceptorId,
        final Runnable poller,
        final int timeoutInMs)
    {
        return Timing.withTimeout(
            "Unable to get session id for: " + initiatorId + " - " + acceptorId,
            () ->
            {
                poller.run();

                return completeSessionIds
                    .stream()
                    .filter((sid) ->
                        sid.remoteCompId().equals(initiatorId) && sid.localCompId().equals(acceptorId))
                    .findFirst();
            },
            timeoutInMs).surrogateId();
    }

    public String lastAcceptorCompId()
    {
        return lastSessionId().localCompId();
    }

    public String lastInitiatorCompId()
    {
        return lastSessionId().remoteCompId();
    }

    public Session lastSession()
    {
        return lastSession;
    }

    private CompleteSessionId lastSessionId()
    {
        return completeSessionIds.peekFirst();
    }

    public boolean isSlow(final Session session)
    {
        return slowSessions.contains(session);
    }

    public boolean lastSessionWasSlow()
    {
        return lastSessionWasSlow;
    }
}
