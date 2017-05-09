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

import org.agrona.ErrorHandler;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.messages.SessionState;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.session.*;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;
import uk.co.real_logic.fix_gateway.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.validation.MessageValidationStrategy;

import java.util.ArrayList;
import java.util.List;

import static uk.co.real_logic.fix_gateway.LogTag.FIX_MESSAGE;

/**
 * Keeps track of which sessions managed by the gateway
 */
class GatewaySessions
{
    private final List<GatewaySession> sessions = new ArrayList<>();
    private final EpochClock clock;
    private final GatewayPublication publication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionCustomisationStrategy customisationStrategy;
    private final FixCounters fixCounters;
    private final AuthenticationStrategy authenticationStrategy;
    private final MessageValidationStrategy validationStrategy;
    private final int sessionBufferSize;
    private final long sendingTimeWindowInMs;
    private final long reasonableTransmissionTimeInMs;
    private ErrorHandler errorHandler;

    GatewaySessions(
        final EpochClock clock,
        final GatewayPublication publication,
        final SessionIdStrategy sessionIdStrategy,
        final SessionCustomisationStrategy customisationStrategy,
        final FixCounters fixCounters,
        final AuthenticationStrategy authenticationStrategy,
        final MessageValidationStrategy validationStrategy,
        final int sessionBufferSize,
        final long sendingTimeWindowInMs,
        final long reasonableTransmissionTimeInMs,
        final ErrorHandler errorHandler)
    {
        this.clock = clock;
        this.publication = publication;
        this.sessionIdStrategy = sessionIdStrategy;
        this.customisationStrategy = customisationStrategy;
        this.fixCounters = fixCounters;
        this.authenticationStrategy = authenticationStrategy;
        this.validationStrategy = validationStrategy;
        this.sessionBufferSize = sessionBufferSize;
        this.sendingTimeWindowInMs = sendingTimeWindowInMs;
        this.reasonableTransmissionTimeInMs = reasonableTransmissionTimeInMs;
        this.errorHandler = errorHandler;
    }

    void acquire(
        final GatewaySession gatewaySession,
        final SessionState state,
        final int heartbeatIntervalInS,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final String username,
        final String password,
        final BlockablePosition engineBlockablePosition)
    {
        final long connectionId = gatewaySession.connectionId();
        final AtomicCounter receivedMsgSeqNo = fixCounters.receivedMsgSeqNo(connectionId);
        final AtomicCounter sentMsgSeqNo = fixCounters.sentMsgSeqNo(connectionId);
        final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer(new byte[sessionBufferSize]);

        final SessionProxy proxy = new SessionProxy(
            asciiBuffer,
            publication,
            sessionIdStrategy,
            customisationStrategy,
            clock,
            connectionId,
            FixEngine.ENGINE_LIBRARY_ID);

        final Session session = new Session(
            heartbeatIntervalInS,
            connectionId,
            clock,
            state,
            proxy,
            publication,
            sessionIdStrategy,
            sendingTimeWindowInMs,
            receivedMsgSeqNo,
            sentMsgSeqNo,
            FixEngine.ENGINE_LIBRARY_ID,
            lastSentSequenceNumber + 1,
            // This gets set by the receiver end point once the logon message has been received.
            0,
            reasonableTransmissionTimeInMs,
            asciiBuffer);

        final SessionParser sessionParser = new SessionParser(
            session,
            sessionIdStrategy,
            authenticationStrategy,
            validationStrategy,
            errorHandler);

        sessions.add(gatewaySession);
        gatewaySession.manage(sessionParser, session, engineBlockablePosition);

        final CompositeKey sessionKey = gatewaySession.sessionKey();
        DebugLogger.log(FIX_MESSAGE, "Gateway Acquired Session %d%n", connectionId);
        if (sessionKey != null)
        {
            gatewaySession.onLogon(
                username,
                password,
                heartbeatIntervalInS);
            session.lastReceivedMsgSeqNum(lastReceivedSequenceNumber);
        }
    }

    GatewaySession releaseBySessionId(final long sessionId)
    {
        final int index = indexBySesionId(sessionId);
        if (index < 0)
        {
            return  null;
        }

        return sessions.remove(index);
    }

    GatewaySession sessionById(final long sessionId)
    {
        final int index = indexBySesionId(sessionId);
        if (index < 0)
        {
            return  null;
        }

        return sessions.get(index);
    }

    private int indexBySesionId(final long sessionId)
    {
        final List<GatewaySession> sessions = this.sessions;

        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final GatewaySession session = sessions.get(i);
            if (session.sessionId() == sessionId)
            {
                return i;
            }
        }

        return -1;
    }

    GatewaySession releaseByConnectionId(final long connectionId)
    {
        final GatewaySession session = removeSessionByConnectionId(connectionId, sessions);
        if (session != null)
        {
            session.close();
        }
        return session;
    }

    int pollSessions(final long time)
    {
        final List<GatewaySession> sessions = this.sessions;

        int eventsProcessed = 0;
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final GatewaySession session = sessions.get(i);
            eventsProcessed += session.poll(time);
        }
        return eventsProcessed;
    }

    List<GatewaySession> sessions()
    {
        return sessions;
    }

    static GatewaySession removeSessionByConnectionId(final long connectionId, final List<GatewaySession> sessions)
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
