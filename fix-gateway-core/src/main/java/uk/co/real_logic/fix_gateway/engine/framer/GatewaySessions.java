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
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
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
public class GatewaySessions
{
    public static final int FRAGMENT_LIMIT = 10;

    private final List<Session> sessions = new ArrayList<>();
    private final Consumer<SessionInfo> onSessionReleaseFunc = this::onSessionRelease;
    private final EpochClock clock;
    private final GatewayPublication publication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionCustomisationStrategy customisationStrategy;
    private final FixCounters fixCounters;
    private final AuthenticationStrategy authenticationStrategy;
    private final MessageValidationStrategy validationStrategy;

    public GatewaySessions(
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

    public void onLibraryTimeout(final LibraryInfo library)
    {
        library.sessions().forEach(onSessionReleaseFunc);
    }

    public void onSessionRelease(final SessionInfo sessionInfo)
    {
        final int libraryId = 0;
        final long connectionId = sessionInfo.connectionId();
        final int sessionBufferSize = sessionInfo.sessionBufferSize();

        final SessionProxy proxy = new SessionProxy(
            sessionBufferSize,
            publication,
            sessionIdStrategy,
            customisationStrategy,
            clock,
            connectionId,
            libraryId
        );

        final AtomicCounter receivedMsgSeqNo = null;
        final AtomicCounter sentMsgSeqNo = null;
        final int sentSequenceNumber = 0;

        final Session session = new Session(
            sessionInfo.heartbeatIntervalInS(),
            connectionId,
            clock,
            SessionState.CONNECTED,
            proxy,
            publication,
            sessionIdStrategy,
            sessionInfo.expectedBeginString(),
            sessionInfo.sendingTimeWindow(),
            receivedMsgSeqNo,
            sentMsgSeqNo,
            libraryId,
            sessionBufferSize,
            sentSequenceNumber
        );

        final SessionParser sessionParser = new SessionParser(
            session,
            sessionIdStrategy,
            authenticationStrategy,
            validationStrategy
        );

        // TODO: push into sessionInfo
    }

    public void onSessionAcquire(final SessionInfo session, final int library)
    {
    }

    public int pollSessions(final long time)
    {
        int eventsProcessed = 0;
        for (final Session session : sessions)
        {
            eventsProcessed += session.poll(time);
        }
        return eventsProcessed;
    }
}
