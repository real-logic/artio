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

import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.EpochClock;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.session.SessionCustomisationStrategy;
import uk.co.real_logic.fix_gateway.session.SessionParser;
import uk.co.real_logic.fix_gateway.session.SessionProxy;
import uk.co.real_logic.fix_gateway.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.validation.MessageValidationStrategy;
import uk.co.real_logic.fix_gateway.messages.SessionState;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Keeps track of which sessions managed by the gateway
 */
public class GatewaySessions
{
    private final List<GatewaySession> sessions = new ArrayList<>();
    private final List<SessionInfo> unmodifiableSessions = unmodifiableList(sessions);
    private final EpochClock clock;
    private final GatewayPublication publication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionCustomisationStrategy customisationStrategy;
    private final FixCounters fixCounters;
    private final AuthenticationStrategy authenticationStrategy;
    private final MessageValidationStrategy validationStrategy;
    private final int sessionBufferSize;

    public GatewaySessions(
        final EpochClock clock,
        final GatewayPublication publication,
        final SessionIdStrategy sessionIdStrategy,
        final SessionCustomisationStrategy customisationStrategy,
        final FixCounters fixCounters,
        final AuthenticationStrategy authenticationStrategy,
        final MessageValidationStrategy validationStrategy,
        final int sessionBufferSize)
    {
        this.clock = clock;
        this.publication = publication;
        this.sessionIdStrategy = sessionIdStrategy;
        this.customisationStrategy = customisationStrategy;
        this.fixCounters = fixCounters;
        this.authenticationStrategy = authenticationStrategy;
        this.validationStrategy = validationStrategy;
        this.sessionBufferSize = sessionBufferSize;
    }

    void acquire(final GatewaySession gatewaySession, final SessionState state, final long heartbeatIntervalInMs)
    {
        final long connectionId = gatewaySession.connectionId();
        final long sessionId = gatewaySession.sessionId();
        final AtomicCounter receivedMsgSeqNo = fixCounters.receivedMsgSeqNo(connectionId);
        final AtomicCounter sentMsgSeqNo = fixCounters.sentMsgSeqNo(connectionId);
        final int sentSequenceNumber = 0; // TODO: lookup
        final int heartbeatIntervalInS = (int) MILLISECONDS.toSeconds(heartbeatIntervalInMs);

        final SessionProxy proxy = new SessionProxy(
            sessionBufferSize,
            publication,
            sessionIdStrategy,
            customisationStrategy,
            clock,
            connectionId,
            FixEngine.GATEWAY_LIBRARY_ID
        );

        final Session session = new Session(
            heartbeatIntervalInS,
            connectionId,
            clock,
            state,
            proxy,
            publication,
            sessionIdStrategy,
            gatewaySession.sendingTimeWindow(),
            receivedMsgSeqNo,
            sentMsgSeqNo,
            FixEngine.GATEWAY_LIBRARY_ID,
            sessionBufferSize,
            sentSequenceNumber
        ).id(sessionId);

        final SessionParser sessionParser = new SessionParser(
            session,
            sessionIdStrategy,
            authenticationStrategy,
            validationStrategy
        );

        sessions.add(gatewaySession);
        gatewaySession.manage(sessionParser, session);
    }

    GatewaySession release(final long connectionId)
    {
        return removeSession(connectionId, sessions);
    }

    int pollSessions(final long time)
    {
        int eventsProcessed = 0;
        for (final GatewaySession session : sessions)
        {
            eventsProcessed += session.poll(time);
        }
        return eventsProcessed;
    }

    List<SessionInfo> sessions()
    {
        return unmodifiableSessions;
    }

    static GatewaySession removeSession(final long connectionId, final List<GatewaySession> sessions)
    {
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final GatewaySession session = sessions.get(i);
            if (session.connectionId() == connectionId)
            {
                sessions.remove(i);
                return session;
            }
        }

        return null;
    }
}
