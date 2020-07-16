/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.engine.framer;

import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.Clock;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.FixCounters;
import uk.co.real_logic.artio.FixGatewayException;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.decoder.AbstractLogonDecoder;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.ByteBufferUtil;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.HeaderSetup;
import uk.co.real_logic.artio.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.artio.fields.EpochFractionFormat;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.library.OnMessageInfo;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.session.*;
import uk.co.real_logic.artio.util.CharFormatter;
import uk.co.real_logic.artio.util.EpochFractionClock;
import uk.co.real_logic.artio.util.EpochFractionClocks;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;
import uk.co.real_logic.artio.validation.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static uk.co.real_logic.artio.LogTag.FIX_CONNECTION;
import static uk.co.real_logic.artio.engine.ConnectedSessionInfo.UNK_SESSION;
import static uk.co.real_logic.artio.engine.framer.SessionContexts.DUPLICATE_SESSION;
import static uk.co.real_logic.artio.engine.framer.SessionContexts.UNKNOWN_SESSION;
import static uk.co.real_logic.artio.validation.SessionPersistenceStrategy.resetSequenceNumbersUponLogon;

/**
 * Keeps track of which sessions managed by the gateway
 */
class GatewaySessions
{
    private final CharFormatter acquiredConnection =
        new CharFormatter("Gateway Acquired Connection %s%n");
    private final List<GatewaySession> sessions = new ArrayList<>();
    private final Map<FixDictionary, UserRequestExtractor> dictionaryToUserRequestExtractor = new HashMap<>();

    private final EpochClock epochClock;
    private final EpochFractionClock epochFractionClock;
    private final GatewayPublication inboundPublication;
    private final GatewayPublication outboundPublication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionCustomisationStrategy customisationStrategy;
    private final FixCounters fixCounters;
    private final AuthenticationStrategy authenticationStrategy;
    private final MessageValidationStrategy validationStrategy;
    private final int sessionBufferSize;
    private final long sendingTimeWindowInMs;
    private final long reasonableTransmissionTimeInMs;
    private final boolean logAllMessages;
    private final boolean validateCompIdsOnEveryMessage;
    private final boolean validateTimeStrictly;
    private final SessionContexts sessionContexts;
    private final SessionPersistenceStrategy sessionPersistenceStrategy;
    private final SequenceNumberIndexReader sentSequenceNumberIndex;
    private final SequenceNumberIndexReader receivedSequenceNumberIndex;
    private final Clock clock;
    private final EpochFractionFormat epochFractionPrecision;
    private final UtcTimestampEncoder sendingTimeEncoder;

    // Initialised after logon processed.
    private SessionContext sessionContext;
    private ErrorHandler errorHandler;

    private final Function<FixDictionary, UserRequestExtractor> newUserRequestExtractor =
        dictionary -> new UserRequestExtractor(dictionary, errorHandler);

    GatewaySessions(
        final EpochClock epochClock,
        final GatewayPublication inboundPublication,
        final GatewayPublication outboundPublication,
        final SessionIdStrategy sessionIdStrategy,
        final SessionCustomisationStrategy customisationStrategy,
        final FixCounters fixCounters,
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final SessionContexts sessionContexts,
        final SessionPersistenceStrategy sessionPersistenceStrategy,
        final SequenceNumberIndexReader sentSequenceNumberIndex,
        final SequenceNumberIndexReader receivedSequenceNumberIndex,
        final EpochFractionFormat epochFractionPrecision)
    {
        this.epochClock = epochClock;
        this.inboundPublication = inboundPublication;
        this.outboundPublication = outboundPublication;
        this.sessionIdStrategy = sessionIdStrategy;
        this.customisationStrategy = customisationStrategy;
        this.fixCounters = fixCounters;
        this.authenticationStrategy = configuration.authenticationStrategy();
        this.validationStrategy = configuration.messageValidationStrategy();
        this.sessionBufferSize = configuration.sessionBufferSize();
        this.sendingTimeWindowInMs = configuration.sendingTimeWindowInMs();
        this.reasonableTransmissionTimeInMs = configuration.reasonableTransmissionTimeInMs();
        this.logAllMessages = configuration.logAllMessages();
        this.validateCompIdsOnEveryMessage = configuration.validateCompIdsOnEveryMessage();
        this.validateTimeStrictly = configuration.validateTimeStrictly();
        this.clock = configuration.clock();
        this.errorHandler = errorHandler;
        this.sessionContexts = sessionContexts;
        this.sessionPersistenceStrategy = sessionPersistenceStrategy;
        this.sentSequenceNumberIndex = sentSequenceNumberIndex;
        this.receivedSequenceNumberIndex = receivedSequenceNumberIndex;
        this.epochFractionPrecision = epochFractionPrecision;
        this.epochFractionClock = EpochFractionClocks.create(epochClock, configuration.epochNanoClock(),
            epochFractionPrecision);

        sendingTimeEncoder = new UtcTimestampEncoder(epochFractionPrecision);
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

    void acquire(
        final GatewaySession gatewaySession,
        final SessionState state,
        final boolean awaitingResend,
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
        final OnMessageInfo messageInfo = new OnMessageInfo();

        final SessionProxy proxy = new DirectSessionProxy(
            sessionBufferSize,
            outboundPublication,
            sessionIdStrategy,
            customisationStrategy,
            epochClock,
            connectionId,
            FixEngine.ENGINE_LIBRARY_ID,
            errorHandler,
            epochFractionPrecision);

        final InternalSession session = new InternalSession(
            heartbeatIntervalInS,
            connectionId,
            epochClock,
            clock,
            state,
            proxy,
            inboundPublication,
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
            asciiBuffer,
            gatewaySession.enableLastMsgSeqNumProcessed(),
            customisationStrategy,
            messageInfo,
            epochFractionClock);

        session.awaitingResend(awaitingResend);
        session.closedResendInterval(gatewaySession.closedResendInterval());
        session.resendRequestChunkSize(gatewaySession.resendRequestChunkSize());
        session.sendRedundantResendRequests(gatewaySession.sendRedundantResendRequests());

        final SessionParser sessionParser = new SessionParser(
            session,
            validationStrategy,
            errorHandler,
            validateCompIdsOnEveryMessage,
            validateTimeStrictly,
            messageInfo,
            sessionIdStrategy);

        if (!sessions.contains(gatewaySession))
        {
            sessions.add(gatewaySession);
        }
        gatewaySession.manage(sessionParser, session, engineBlockablePosition);

        if (DebugLogger.isEnabled(FIX_CONNECTION))
        {
            DebugLogger.log(FIX_CONNECTION, acquiredConnection.clear().with(connectionId));
        }
        final CompositeKey sessionKey = gatewaySession.sessionKey();
        if (sessionKey != null)
        {
            gatewaySession.updateSessionDictionary();
            gatewaySession.onLogon(username, password, heartbeatIntervalInS);
            session.initialLastReceivedMsgSeqNum(lastReceivedSequenceNumber);
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

        return indexBySessionId(sessionId, sessions);
    }

    static int indexBySessionId(final long sessionId, final List<GatewaySession> sessions)
    {
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final GatewaySession session = sessions.get(i);
            if (session.sessionId() == sessionId)
            {
                return i;
            }
        }

        return UNK_SESSION;
    }

    void releaseByConnectionId(final long connectionId)
    {
        final GatewaySession session = removeSessionByConnectionId(connectionId, sessions);
        if (session != null)
        {
            session.close();
        }
    }

    int pollSessions(final long time)
    {
        final List<GatewaySession> sessions = this.sessions;

        int eventsProcessed = 0;
        for (int i = 0, size = sessions.size(); i < size;)
        {
            final GatewaySession session = sessions.get(i);
            eventsProcessed += session.poll(time);
            if (session.hasDisconnected())
            {
                size--;
            }
            else
            {
                i++;
            }
        }
        return eventsProcessed;
    }

    List<GatewaySession> sessions()
    {
        return sessions;
    }

    AcceptorLogonResult authenticate(
        final AbstractLogonDecoder logon,
        final long connectionId,
        final GatewaySession gatewaySession,
        final TcpChannel channel,
        final FixDictionary fixDictionary,
        final Framer framer)
    {
        gatewaySession.startAuthentication(epochClock.time());

        return new PendingAcceptorLogon(
            sessionIdStrategy, gatewaySession, logon, connectionId, sessionContexts, channel, fixDictionary, framer);
    }

    private boolean lookupSequenceNumbers(final GatewaySession gatewaySession, final long requiredPosition)
    {
        final int aeronSessionId = outboundPublication.id();
        final long initialPosition = outboundPublication.initialPosition();
        // At requiredPosition=initialPosition there won't be anything indexed, so indexedPosition will be -1
        if (requiredPosition > initialPosition)
        {
            final long indexedPosition = sentSequenceNumberIndex.indexedPosition(aeronSessionId);
            if (indexedPosition < requiredPosition)
            {
                return false;
            }
        }

        final long sessionId = gatewaySession.sessionId();
        final int lastSentSequenceNumber = sentSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
        final int lastReceivedSequenceNumber = receivedSequenceNumberIndex.lastKnownSequenceNumber(sessionId);
        gatewaySession.acceptorSequenceNumbers(lastSentSequenceNumber, lastReceivedSequenceNumber);
        if (lastReceivedSequenceNumber != UNK_SESSION)
        {
            gatewaySession.lastSequenceResetTime(sessionContext.lastSequenceResetTime());
        }
        return true;
    }

    void onUserRequest(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final FixDictionary dictionary,
        final long connectionId,
        final long sessionId)
    {
        final UserRequestExtractor extractor = dictionaryToUserRequestExtractor
            .computeIfAbsent(dictionary, newUserRequestExtractor);

        extractor.onUserRequest(buffer, offset, length, authenticationStrategy, connectionId, sessionId);
    }

    // We put the gateway session in our list of sessions to poll in order to check engine level timeouts,
    // But we aren't actually acquiring the session.
    void track(final GatewaySession gatewaySession)
    {
        sessions.add(gatewaySession);
    }

    enum AuthenticationState
    {
        PENDING,
        AUTHENTICATED,
        INDEXER_CATCHUP,
        ACCEPTED,
        SENDING_REJECT_MESSAGE,
        LINGERING_REJECT_MESSAGE,
        REJECTED
    }

    private final class PendingAcceptorLogon implements AuthenticationProxy, AcceptorLogonResult
    {
        private static final long NO_REQUIRED_POSITION = -1;
        private static final int ENCODE_BUFFER_SIZE = 1024;

        private final SessionIdStrategy sessionIdStrategy;
        private final AbstractLogonDecoder logon;
        private final SessionContexts sessionContexts;
        private final TcpChannel channel;
        private FixDictionary fixDictionary;
        private final Framer framer;
        private final boolean resetSeqNum;

        private volatile AuthenticationState state = AuthenticationState.PENDING;

        private GatewaySession session;
        private DisconnectReason reason;
        private long requiredPosition = NO_REQUIRED_POSITION;
        private long lingerTimeoutInMs;

        private Encoder encoder;
        private ByteBuffer encodeBuffer;
        private long lingerExpiryTimeInMs;
        private Class<? extends FixDictionary> fixDictionaryClass;

        PendingAcceptorLogon(
            final SessionIdStrategy sessionIdStrategy,
            final GatewaySession gatewaySession,
            final AbstractLogonDecoder logon,
            final long connectionId,
            final SessionContexts sessionContexts,
            final TcpChannel channel,
            final FixDictionary fixDictionary,
            final Framer framer)
        {
            this.sessionIdStrategy = sessionIdStrategy;
            this.session = gatewaySession;
            this.logon = logon;
            this.sessionContexts = sessionContexts;
            this.channel = channel;
            this.fixDictionary = fixDictionary;
            this.framer = framer;

            final PersistenceLevel persistenceLevel = getPersistenceLevel(logon, connectionId);
            final boolean resetSeqNumFlag = logon.hasResetSeqNumFlag() && logon.resetSeqNumFlag();

            final boolean resetSequenceNumbersUponLogon = resetSequenceNumbersUponLogon(persistenceLevel);
            resetSeqNum = resetSequenceNumbersUponLogon || resetSeqNumFlag;

            if (!resetSequenceNumbersUponLogon && !logAllMessages)
            {
                onError(new IllegalStateException(
                    "Persistence Strategy specified INDEXED but " +
                    "EngineConfiguration has disabled required logging of messsages"));

                reject(DisconnectReason.INVALID_CONFIGURATION_NOT_LOGGING_MESSAGES);
                return;
            }

            authenticate(logon, connectionId);
        }

        private PersistenceLevel getPersistenceLevel(final AbstractLogonDecoder logon, final long connectionId)
        {
            try
            {
                return sessionPersistenceStrategy.getPersistenceLevel(logon);
            }
            catch (final Throwable throwable)
            {
                onStrategyError(
                    "persistence", throwable, connectionId, "TRANSIENT_SEQUENCE_NUMBERS", logon);
                return PersistenceLevel.TRANSIENT_SEQUENCE_NUMBERS;
            }
        }

        private void authenticate(final AbstractLogonDecoder logon, final long connectionId)
        {
            try
            {
                authenticationStrategy.authenticateAsync(logon, this);
            }
            catch (final Throwable throwable)
            {
                onStrategyError("authentication", throwable, connectionId, "false", logon);

                if (state != AuthenticationState.REJECTED)
                {
                    reject();
                }
            }
        }

        private void onStrategyError(
            final String strategyName,
            final Throwable throwable,
            final long connectionId,
            final String theDefault,
            final AbstractLogonDecoder logon)
        {
            final String message = String.format(
                "Exception thrown by %s strategy for connectionId=%d, processing [%s], defaulted to %s",
                strategyName,
                connectionId,
                logon.toString(),
                theDefault);
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

        public DisconnectReason reason()
        {
            return reason;
        }

        public void accept()
        {
            validateState();

            state = AuthenticationState.AUTHENTICATED;
        }

        public void accept(final Class<? extends FixDictionary> fixDictionaryClass)
        {
            validateState();

            this.fixDictionaryClass = fixDictionaryClass;
            state = AuthenticationState.AUTHENTICATED;
        }

        private void validateState()
        {
            // NB: simple best efforts state check to catch programming errors.
            // Technically can race if two different threads call accept and reject at the exact same moment.
            final AuthenticationState state = this.state;

            if (!(state == AuthenticationState.PENDING || state == AuthenticationState.AUTHENTICATED))
            {
                throw new IllegalStateException(String.format(
                    "Cannot reject and accept a pending operation at the same time (state=%s)", state));
            }
        }

        public boolean poll()
        {
            switch (state)
            {
                case AUTHENTICATED:
                    if (fixDictionaryClass != null && fixDictionary.getClass() != fixDictionaryClass)
                    {
                        fixDictionary = FixDictionary.of(fixDictionaryClass);
                        session.fixDictionary(fixDictionary);
                    }

                    session.onAuthenticationResult();
                    onAuthenticated();
                    return false;

                case ACCEPTED:
                    return true;

                case REJECTED:
                    checkedOnAuthenticationResult();
                    return true;

                case SENDING_REJECT_MESSAGE:
                    checkedOnAuthenticationResult();
                    return onSendingRejectMessage();

                case LINGERING_REJECT_MESSAGE:
                    return onLingerRejectMessage();

                case INDEXER_CATCHUP:
                    onIndexerCatchup();
                    return false;

                case PENDING:
                default:
                    return false;
            }
        }

        private void checkedOnAuthenticationResult()
        {
            if (session != null)
            {
                session.onAuthenticationResult();
                session = null;
            }
        }

        private boolean onLingerRejectMessage()
        {
            final long timeInMs = epochClock.time();
            final boolean complete = timeInMs >= lingerExpiryTimeInMs;

            if (complete)
            {
                state = AuthenticationState.REJECTED;
            }

            return complete;
        }

        private boolean onSendingRejectMessage()
        {
            if (encodeBuffer == null)
            {
                try
                {
                    encodeRejectMessage();
                }
                catch (final Exception e)
                {
                    errorHandler.onError(e);
                    state = AuthenticationState.REJECTED;
                    return true;
                }
            }

            try
            {
                channel.write(encodeBuffer);
                if (!encodeBuffer.hasRemaining())
                {
                    lingerExpiryTimeInMs = epochClock.time() + lingerTimeoutInMs;
                    state = AuthenticationState.LINGERING_REJECT_MESSAGE;
                }
            }
            catch (final IOException e)
            {
                // The TCP Connection has disconnected, therefore we consider this complete.
                state = AuthenticationState.REJECTED;
                return true;
            }

            return false;
        }

        private void encodeRejectMessage()
        {
            encodeBuffer = ByteBuffer.allocateDirect(ENCODE_BUFFER_SIZE);

            final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer(encodeBuffer);

            final SessionHeaderEncoder header = encoder.header();
            header.msgSeqNum(1);
            header.sendingTime(
                sendingTimeEncoder.buffer(), sendingTimeEncoder.encodeFrom(epochClock.time(), TimeUnit.MILLISECONDS));
            HeaderSetup.setup(logon.header(), header);
            customisationStrategy.configureHeader(header, UNKNOWN_SESSION.sessionId());

            final long result = encoder.encode(asciiBuffer, 0);
            final int offset = Encoder.offset(result);
            final int length = Encoder.length(result);

            ByteBufferUtil.position(encodeBuffer, offset);
            ByteBufferUtil.limit(encodeBuffer, offset + length);
        }

        private void onIndexerCatchup()
        {
            if (lookupSequenceNumbers(session, requiredPosition))
            {
                state = AuthenticationState.ACCEPTED;
            }
        }

        private void onAuthenticated()
        {
            final String username = SessionParser.username(logon);
            final String password = SessionParser.password(logon);

            final SessionHeaderDecoder header = logon.header();
            final CompositeKey compositeKey = sessionIdStrategy.onAcceptLogon(header);
            sessionContext = sessionContexts.onLogon(compositeKey, fixDictionary);

            if (sessionContext == DUPLICATE_SESSION)
            {
                reject(DisconnectReason.DUPLICATE_SESSION);
                return;
            }

            final boolean isOfflineReconnect = framer.onLogonMessageReceived(session, sessionContext.sessionId());

            final long logonTime = clock.time();
            sessionContext.onLogon(resetSeqNum, logonTime, fixDictionary);
            session.initialResetSeqNum(resetSeqNum);
            session.fixDictionary(fixDictionary);
            session.updateSessionDictionary();
            session.onLogon(
                sessionContext.sessionId(),
                sessionContext,
                compositeKey,
                username,
                password,
                logon.heartBtInt(),
                header.msgSeqNum());
            session.lastLogonTime(logonTime);

            // See Framer.handoverNewConnectionToLibrary for sole library mode equivalent
            if (resetSeqNum)
            {
                session.acceptorSequenceNumbers(UNK_SESSION, UNK_SESSION);
                session.lastLogonWasSequenceReset();
                state = AuthenticationState.ACCEPTED;
            }
            else
            {
                requiredPosition = outboundPublication.position();
                state = AuthenticationState.INDEXER_CATCHUP;
            }

            framer.onGatewaySessionSetup(session, isOfflineReconnect);
        }

        public void reject()
        {
            validateState();

            reject(DisconnectReason.FAILED_AUTHENTICATION);
        }

        public void reject(final Encoder encoder, final long lingerTimeoutInMs)
        {
            Objects.requireNonNull(encoder, "encoder should be provided");

            if (lingerTimeoutInMs < 0)
            {
                throw new IllegalArgumentException(String.format(
                    "lingerTimeoutInMs should not be negative, (%d)", lingerTimeoutInMs));
            }

            this.encoder = encoder;
            this.reason = DisconnectReason.FAILED_AUTHENTICATION;
            this.lingerTimeoutInMs = lingerTimeoutInMs;
            this.state = AuthenticationState.SENDING_REJECT_MESSAGE;
        }

        public String remoteAddress()
        {
            return channel.remoteAddress();
        }

        private void reject(final DisconnectReason reason)
        {
            validateState();

            this.reason = reason;
            this.state = AuthenticationState.REJECTED;
        }

        @Override
        public boolean isAccepted()
        {
            return AuthenticationState.ACCEPTED == state;
        }
    }
}
