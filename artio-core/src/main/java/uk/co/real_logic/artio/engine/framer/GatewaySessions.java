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
package uk.co.real_logic.artio.engine.framer;

import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.FixCounters;
import uk.co.real_logic.artio.FixGatewayException;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.session.*;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;
import uk.co.real_logic.artio.validation.PersistenceLevel;
import uk.co.real_logic.artio.validation.SessionPersistenceStrategy;

import java.util.ArrayList;
import java.util.List;

import static uk.co.real_logic.artio.LogTag.FIX_MESSAGE;
import static uk.co.real_logic.artio.engine.framer.SessionContexts.DUPLICATE_SESSION;
import static uk.co.real_logic.artio.validation.SessionPersistenceStrategy.resetSequenceNumbersUponLogon;

/**
 * Keeps track of which sessions managed by the gateway
 */
class GatewaySessions
{
    private final List<GatewaySession> sessions = new ArrayList<>();
    private final EpochClock clock;
    private final GatewayPublication outboundPublication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionCustomisationStrategy customisationStrategy;
    private final FixCounters fixCounters;
    private final AuthenticationStrategy authenticationStrategy;
    private final MessageValidationStrategy validationStrategy;
    private final int sessionBufferSize;
    private final long sendingTimeWindowInMs;
    private final long reasonableTransmissionTimeInMs;
    private final SessionContexts sessionContexts;
    private final SessionPersistenceStrategy sessionPersistenceStrategy;

    private ErrorHandler errorHandler;

    GatewaySessions(
        final EpochClock clock,
        final GatewayPublication outboundPublication,
        final SessionIdStrategy sessionIdStrategy,
        final SessionCustomisationStrategy customisationStrategy,
        final FixCounters fixCounters,
        final AuthenticationStrategy authenticationStrategy,
        final MessageValidationStrategy validationStrategy,
        final int sessionBufferSize,
        final long sendingTimeWindowInMs,
        final long reasonableTransmissionTimeInMs,
        final ErrorHandler errorHandler,
        final SessionContexts sessionContexts,
        final SessionPersistenceStrategy sessionPersistenceStrategy)
    {
        this.clock = clock;
        this.outboundPublication = outboundPublication;
        this.sessionIdStrategy = sessionIdStrategy;
        this.customisationStrategy = customisationStrategy;
        this.fixCounters = fixCounters;
        this.authenticationStrategy = authenticationStrategy;
        this.validationStrategy = validationStrategy;
        this.sessionBufferSize = sessionBufferSize;
        this.sendingTimeWindowInMs = sendingTimeWindowInMs;
        this.reasonableTransmissionTimeInMs = reasonableTransmissionTimeInMs;
        this.errorHandler = errorHandler;
        this.sessionContexts = sessionContexts;
        this.sessionPersistenceStrategy = sessionPersistenceStrategy;
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
            outboundPublication,
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
            outboundPublication,
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
            sessionIdStrategy, validationStrategy,
            errorHandler);

        sessions.add(gatewaySession);
        gatewaySession.manage(sessionParser, session, engineBlockablePosition);

        final CompositeKey sessionKey = gatewaySession.sessionKey();
        DebugLogger.log(FIX_MESSAGE, "Gateway Acquired Session %d%n", connectionId);
        if (sessionKey != null)
        {
            gatewaySession.onLogon(username, password, heartbeatIntervalInS);
            session.lastReceivedMsgSeqNum(lastReceivedSequenceNumber);
        }
    }

    GatewaySession releaseBySessionId(final long sessionId)
    {
        final int index = indexBySessionId(sessionId);
        if (index < 0)
        {
            return null;
        }

        return sessions.remove(index);
    }

    GatewaySession sessionById(final long sessionId)
    {
        final int index = indexBySessionId(sessionId);
        if (index < 0)
        {
            return null;
        }

        return sessions.get(index);
    }

    private int indexBySessionId(final long sessionId)
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

    AuthenticationResult authenticateAndInitiate(
        final LogonDecoder logon,
        final long connectionId,
        final SequenceNumberIndexReader sentSequenceNumberIndex,
        final SequenceNumberIndexReader receivedSequenceNumberIndex,
        final GatewaySession gatewaySession)
    {
        final CompositeKey compositeKey = sessionIdStrategy.onAcceptLogon(logon.header());
        final SessionContext sessionContext = sessionContexts.onLogon(compositeKey);
        final long sessionId = sessionContext.sessionId();
        if (sessionContext == DUPLICATE_SESSION)
        {
            return AuthenticationResult.DUPLICATE_SESSION;
        }

        boolean authenticated;
        try
        {
            authenticated = authenticationStrategy.authenticate(logon);
        }
        catch (final Throwable throwable)
        {
            // TODO(Nick): Maybe this should go back to also logging the message that was being decoded.
            onStrategyError("authentication", throwable, connectionId);
            authenticated = false;
        }

        if (!authenticated)
        {
            return AuthenticationResult.FAILED_AUTHENTICATION;
        }

        PersistenceLevel persistenceLevel;
        try
        {
            persistenceLevel = sessionPersistenceStrategy.getPersistenceLevel(logon);
        }
        catch (final Throwable throwable)
        {
            final String message = String.format(
                "Exception thrown by persistence strategy for connectionId=%d, defaulted to UNINDEXED",
                connectionId);
            errorHandler.onError(new FixGatewayException(message, throwable));
            persistenceLevel = PersistenceLevel.UNINDEXED;
        }

        final boolean resetSeqNumFlag = logon.hasResetSeqNumFlag() && logon.resetSeqNumFlag();
        final boolean resetSeqNum = resetSequenceNumbersUponLogon(persistenceLevel) || resetSeqNumFlag;

        final int aeronSessionId = outboundPublication.id();
        final long requiredPosition = outboundPublication.position();
        // At requiredPosition=0 there won't be anything indexed, so indexedPosition will be -1
        if (requiredPosition > 0)
        {
            while (sentSequenceNumberIndex.indexedPosition(aeronSessionId) < requiredPosition)
            {
                Thread.yield(); // TODO: better idling
            }
        }

        final int lastSentSequenceNumber = sequenceNumber(sentSequenceNumberIndex, resetSeqNum, sessionId);
        final int lastReceivedSequenceNumber = sequenceNumber(receivedSequenceNumberIndex, resetSeqNum, sessionId);

        final String username = SessionParser.username(logon);
        final String password = SessionParser.password(logon);

        sessionContext.onLogon(resetSeqNum);

        gatewaySession.onLogon(sessionId, sessionContext, compositeKey, username, password, logon.heartBtInt());
        gatewaySession.acceptorSequenceNumbers(lastSentSequenceNumber, lastReceivedSequenceNumber);
        gatewaySession.persistenceLevel(persistenceLevel);

        return AuthenticationResult.authenticatedSession(gatewaySession);
    }

    private int sequenceNumber(
        final SequenceNumberIndexReader sequenceNumberIndexReader,
        final boolean resetSeqNum,
        final long sessionId)
    {
        if (resetSeqNum)
        {
            return SessionInfo.UNK_SESSION;
        }

        return sequenceNumberIndexReader.lastKnownSequenceNumber(sessionId);
    }

    private void onStrategyError(final String strategyName, final Throwable throwable, final long connectionId)
    {
        final String message = String.format(
            "Exception thrown by %s strategy for connectionId=%d, [%s], defaulted to false",
            strategyName,
            connectionId);
        onError(new FixGatewayException(message, throwable));
    }

    private void onError(final Throwable throwable)
    {
        // Library code should throw the exception to make users aware of it
        // Engine code should log it through the normal error handling process.
        if (errorHandler == null)
        {
            LangUtil.rethrowUnchecked(throwable);
        }
        else
        {
            errorHandler.onError(throwable);
        }
    }
}
