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
import uk.co.real_logic.fix_gateway.library.session.*;
import uk.co.real_logic.fix_gateway.library.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.MessageValidationStrategy;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.streams.GatewayPublication;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Keeps track of which sessions managed by the gateway
 */
class GatewaySessions
{

    static final int GATEWAY_LIBRARY_ID = 0;

    private final List<Session> sessions = new ArrayList<>();
    private final Consumer<GatewaySession> startManagingFunc = this::startManaging;
    private final EpochClock clock;
    private final GatewayPublication publication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionCustomisationStrategy customisationStrategy;
    private final FixCounters fixCounters;
    private final AuthenticationStrategy authenticationStrategy;
    private final MessageValidationStrategy validationStrategy;

    GatewaySessions(
        final EpochClock clock,
        final GatewayPublication publication,
        final SessionIdStrategy sessionIdStrategy,
        final SessionCustomisationStrategy customisationStrategy,
        final FixCounters fixCounters,
        final AuthenticationStrategy authenticationStrategy,
        final MessageValidationStrategy validationStrategy)
    {
        this.clock = clock;
        this.publication = publication;
        this.sessionIdStrategy = sessionIdStrategy;
        this.customisationStrategy = customisationStrategy;
        this.fixCounters = fixCounters;
        this.authenticationStrategy = authenticationStrategy;
        this.validationStrategy = validationStrategy;
    }

    void onLibraryTimeout(final LibraryInfo library)
    {
        library.gatewaySessions().forEach(startManagingFunc);
    }

    void startManaging(final GatewaySession gatewaySession)
    {
        final long connectionId = gatewaySession.connectionId();
        final int sessionBufferSize = gatewaySession.sessionBufferSize();

        final SessionProxy proxy = new SessionProxy(
            sessionBufferSize,
            publication,
            sessionIdStrategy,
            customisationStrategy,
            clock,
            connectionId,
            GATEWAY_LIBRARY_ID
        );

        final AtomicCounter receivedMsgSeqNo = fixCounters.receivedMsgSeqNo(connectionId);
        final AtomicCounter sentMsgSeqNo = fixCounters.sentMsgSeqNo(connectionId);
        final int sentSequenceNumber = 0; // TODO: lookup

        final Session session = new Session(
            gatewaySession.heartbeatIntervalInS(),
            connectionId,
            clock,
            SessionState.CONNECTED,
            proxy,
            publication,
            sessionIdStrategy,
            gatewaySession.expectedBeginString(),
            gatewaySession.sendingTimeWindow(),
            receivedMsgSeqNo,
            sentMsgSeqNo,
            GATEWAY_LIBRARY_ID,
            sessionBufferSize,
            sentSequenceNumber
        );

        final SessionParser sessionParser = new SessionParser(
            session,
            sessionIdStrategy,
            authenticationStrategy,
            validationStrategy
        );

        sessions.add(session);
        gatewaySession.manage(sessionParser);
    }

    void stopManaging(final GatewaySession gatewaySession)
    {
        sessions.removeIf(session -> session.connectionId() == gatewaySession.connectionId());
        gatewaySession.manage(null);
    }

    int pollSessions(final long time)
    {
        int eventsProcessed = 0;
        for (final Session session : sessions)
        {
            eventsProcessed += session.poll(time);
        }
        return eventsProcessed;
    }
}
