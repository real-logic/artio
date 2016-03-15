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
package uk.co.real_logic.fix_gateway.library;

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.EpochClock;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.SystemEpochClock;
import uk.co.real_logic.agrona.concurrent.SystemNanoClock;
import uk.co.real_logic.fix_gateway.*;
import uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.fix_gateway.library.session.*;
import uk.co.real_logic.fix_gateway.library.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.MessageValidationStrategy;
import uk.co.real_logic.fix_gateway.messages.ConnectionType;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.messages.GatewayError;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.streams.DataSubscriber;
import uk.co.real_logic.fix_gateway.streams.GatewayPublication;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.fix_gateway.messages.GatewayError.UNABLE_TO_CONNECT;

/**
 * FIX Library instances represent a process in the gateway where session management,
 * message parsing and API users configure the gateway.
 * <p>
 * Libraries can be run in the same process as the engine, or in a
 * different process.
 * <p>
 * FixLibrary instances are not thread safe and should be run on
 * their own thread.
 *
 * @see uk.co.real_logic.fix_gateway.engine.FixEngine
 */
public final class FixLibrary extends GatewayProcess
{
    private final Subscription inboundSubscription;
    private final GatewayPublication outboundPublication;
    private final Long2ObjectHashMap<SessionSubscriber> connectionIdToSession = new Long2ObjectHashMap<>();
    private final List<Session> sessions = new ArrayList<>();
    private final List<Session> unmodifiableSessions = unmodifiableList(sessions);

    private final EpochClock clock;
    private final LibraryConfiguration configuration;
    private final SessionIdStrategy sessionIdStrategy;
    private final Timer sessionTimer = new Timer("Session", new SystemNanoClock());
    private final Timer receiveTimer = new Timer("Receive", new SystemNanoClock());
    private final LivenessDetector livenessDetector;
    private final int libraryId;
    private final IdleStrategy idleStrategy;
    private final boolean isAcceptor;

    private Session incomingSession;

    private SessionConfiguration sessionConfiguration;
    private GatewayError errorType;

    private String errorMessage;

    private FixLibrary(final LibraryConfiguration configuration)
    {
        configuration.conclude();

        init(configuration);

        this.configuration = configuration;
        this.sessionIdStrategy = configuration.sessionIdStrategy();
        this.libraryId = configuration.libraryId();
        idleStrategy = configuration.libraryIdleStrategy();
        isAcceptor = configuration.isAcceptor();

        inboundSubscription = inboundLibraryStreams.subscription();
        outboundPublication = outboundLibraryStreams.gatewayPublication(idleStrategy);

        clock = new SystemEpochClock();
        livenessDetector = LivenessDetector.forLibrary(
            outboundPublication,
            configuration.libraryId(),
            configuration.replyTimeoutInMs());
    }

    private FixLibrary connect()
    {
        try
        {
            outboundPublication.saveLibraryConnect(libraryId, isAcceptor);

            final long latestReplyArrivalTime = latestReplyArrivalTime();
            while (!livenessDetector.isConnected() && errorType == null)
            {
                final int workCount = poll(1);

                checkTime(latestReplyArrivalTime);

                idleStrategy.idle(workCount);
            }

            if (errorType != null)
            {
                throw new IllegalArgumentException(String.format(
                        "Unable to connect to engine: %s", errorType
                ));
            }
        }
        catch (Exception e)
        {
            // We won't be returning an instance of ourselves to callers in the connect,
            // so we must clean up after ourselves
            try
            {
                close();
            }
            catch (Exception closeException)
            {
                e.addSuppressed(closeException);
            }

            LangUtil.rethrowUnchecked(e);
        }

        return this;
    }

    // ------------- Public API -------------

    /**
     * Connect to an engine. This method blocks until the connection is complete and then returns.
     *
     * @param configuration the configuration for this library instance.
     * @return the library instance once it has connected.
     * @throws IllegalStateException
     *         if there's an error connecting to the FIX Gateway or if there's a timeout talking to
     *         the FixEngine.
     */
    public static FixLibrary connect(final LibraryConfiguration configuration)
    {
        return new FixLibrary(configuration).connect();
    }

    /**
     * Poll the library all of its component sessions to process any messages
     * and events that have received from or should be sent to the engine.
     *
     * @param fragmentLimit the maximum number of events to read from the engine.
     * @return 0 if no work was performed, > 0 otherwise.
     */
    public int poll(final int fragmentLimit)
    {
        final long timeInMs = clock.time();
        return inboundSubscription.poll(dataSubscriber, fragmentLimit) +
               pollSessions(timeInMs) +
               livenessDetector.poll(timeInMs);
    }

    /**
     * Check if the library is connected to an engine.
     * <p>
     * Note that this refers to whether a library is connected to a FIX Engine,
     * not whether of its sessions are connected.
     *
     * @return true if the library is connected to an engine, false otherwise.
     * @see Session#isConnected()
     * @see uk.co.real_logic.fix_gateway.engine.FixEngine
     */
    public boolean isConnected()
    {
        return livenessDetector.isConnected();
    }

    /**
     * Get the identifier of the library.
     *
     * @return the identifier of the library.
     */
    public int libraryId()
    {
        return libraryId;
    }

    /**
     * Get a list of the currently active sessions.
     * <p>
     * Note: the list is unmodifiable.
     *
     * @return a list of the currently active sessions.
     */
    public List<Session> sessions()
    {
        return unmodifiableSessions;
    }

    /**
     * Close the Library.
     */
    public void close()
    {
        connectionIdToSession.values().forEach(SessionSubscriber::close);
        super.close();
    }

    /**
     * Initiate a FIX session with a FIX acceptor. This method blocks and returns only once the Session
     * object has connected to the acceptor.
     *
     * @param configuration the configuration to use for the session.
     * @param idleStrategy an idle strategy to use for backoffs whilst awaiting the session's acceptance.
     * @return the session object for the session that you've initiated.
     * @throws IllegalStateException
     *         if you're trying to initiate two sessions at the same time or if there's a timeout talking to
     *         the FixEngine.
     *         This probably indicates that there's a problem in your code or that your engine isn't running.
     * @throws FixGatewayException
     *         if you're unable to connect to the accepting gateway.
     *         This probably indicates a configuration problem related to the external gateway.
     */
    public Session initiate(final SessionConfiguration configuration, final IdleStrategy idleStrategy)
    {
        if (sessionConfiguration != null || incomingSession != null || errorType != null)
        {
            throw new IllegalStateException("You can't initiate a session whilst initiating a session");
        }

        sessionConfiguration = configuration;

        try
        {
            final List<String> hosts = configuration.hosts();
            final List<Integer> ports = configuration.ports();
            final int size = hosts.size();
            for (int i = 0; i < size; i++)
            {
                final String host = hosts.get(i);
                final int port = ports.get(i);

                outboundPublication.saveInitiateConnection(
                    libraryId,
                    host,
                    port,
                    configuration.senderCompId(),
                    configuration.senderSubId(),
                    configuration.senderLocationId(),
                    configuration.targetCompId());

                final long latestReplyArrivalTime = latestReplyArrivalTime();
                while (incomingSession == null && errorType == null)
                {
                    final int workCount = poll(5);

                    checkTime(latestReplyArrivalTime);

                    idleStrategy.idle(workCount);
                }

                if (incomingSession != null)
                {
                    final Session session = incomingSession;
                    session.address(host, port);
                    return session;
                }
                else if (errorType != UNABLE_TO_CONNECT)
                {
                    throw new FixGatewayException(String.format("%s: %s", errorType, errorMessage));
                }

                errorType = null;
            }

            throw new FixGatewayException("Unable to connect to any of the addresses specified");
        }
        finally
        {
            sessionConfiguration = null;
            errorType = null;
            incomingSession = null;
        }
    }

    private long latestReplyArrivalTime()
    {
        return clock.time() + configuration.replyTimeoutInMs();
    }

    private void checkTime(final long latestReplyArrivalTime)
    {
        if (clock.time() > latestReplyArrivalTime)
        {
            throw new IllegalStateException(String.format(
                "Failed to receive a reply from the engine within %dms, are you sure its running?",
                this.configuration.replyTimeoutInMs()));
        }
    }

    // ------------- End Public API -------------

    private int pollSessions(final long timeInMs)
    {
        final List<Session> sessions = this.sessions;
        int total = 0;

        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            final Session session = sessions.get(i);
            total += session.poll(timeInMs);
        }

        return total;
    }

    private final DataSubscriber dataSubscriber = new DataSubscriber(new SessionHandler()
    {
        private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();

        public void onConnect(
            final int libraryId,
            final long connectionId,
            final ConnectionType type,
            final int lastSentSequenceNumber,
            final int lastReceivedSequenceNumber,
            final DirectBuffer buffer,
            final int addressOffset,
            final int addressLength)
        {
            if (libraryId == FixLibrary.this.libraryId)
            {
                if (type == INITIATOR)
                {
                    DebugLogger.log("Init Connect: %d, %d\n", connectionId, libraryId);
                    final Session session = initiateSession(
                        connectionId, lastSentSequenceNumber, lastReceivedSequenceNumber);
                    newSession(connectionId, session);
                    incomingSession = session;
                }
                else
                {
                    DebugLogger.log("Acct Connect: %d, %d\n", connectionId, libraryId);
                    asciiBuffer.wrap(buffer);
                    final String address = asciiBuffer.getAscii(addressOffset, addressLength);
                    if (isAcceptor)
                    {
                        final Session session = acceptSession(connectionId, lastSentSequenceNumber, address);
                        newSession(connectionId, session);
                    }
                    else
                    {
                        throw new IllegalArgumentException(String.format(
                            "Received accept for session %d connecting from %s on non-accepting FIX Library",
                            connectionId,
                            address
                        ));
                    }
                }
            }
        }

        public void onLogon(
            final int libraryId,
            final long connectionId,
            final long sessionId,
            int lastSentSequenceNumber,
            int lastReceivedSequenceNumber)
        {
            if (libraryId == FixLibrary.this.libraryId)
            {
                DebugLogger.log("Library Logon: %d, %d\n", connectionId, sessionId);
                final SessionSubscriber subscriber = connectionIdToSession.get(connectionId);
                if (subscriber != null)
                {
                    lastSentSequenceNumber = acceptorSequenceNumber(lastSentSequenceNumber);
                    lastReceivedSequenceNumber = acceptorSequenceNumber(lastReceivedSequenceNumber);
                    subscriber.onLogon(connectionId, sessionId, lastSentSequenceNumber, lastReceivedSequenceNumber);
                }
            }
        }

        private int acceptorSequenceNumber(int lastSequenceNumber)
        {
            if (!configuration.acceptorSequenceNumbersResetUponReconnect() &&
                lastSequenceNumber != SequenceNumberIndexReader.UNKNOWN_SESSION)
            {
                return lastSequenceNumber;
            }

            return 1;
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
            if (libraryId == FixLibrary.this.libraryId)
            {
                DebugLogger.log("Received %s\n", buffer, offset, length);
                final SessionSubscriber subscriber = connectionIdToSession.get(connectionId);
                if (subscriber != null)
                {
                    subscriber.onMessage(
                        buffer, offset, length, libraryId, connectionId, sessionId, messageType, timestamp);
                }
            }
        }

        public void onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
        {
            if (libraryId == FixLibrary.this.libraryId)
            {
                final SessionSubscriber subscriber = connectionIdToSession.remove(connectionId);
                DebugLogger.log("Library Disconnect %d, %s\n", connectionId, reason);
                if (subscriber != null)
                {
                    subscriber.onDisconnect(libraryId, connectionId, reason);
                    final Session session = subscriber.session();
                    sessions.remove(session);
                }
            }
        }

        public void onError(final GatewayError errorType, final int libraryId, final String message)
        {
            if (libraryId == FixLibrary.this.libraryId)
            {
                FixLibrary.this.errorType = errorType;
                FixLibrary.this.errorMessage = message;
            }
        }

        public void onApplicationHeartbeat(final int libraryId)
        {
            if (libraryId == FixLibrary.this.libraryId)
            {
                livenessDetector.onHeartbeat(clock.time());
            }
        }
    });

    private void newSession(final long connectionId, final Session session)
    {
        final AuthenticationStrategy authenticationStrategy = configuration.authenticationStrategy();
        final MessageValidationStrategy validationStrategy = configuration.messageValidationStrategy();
        final SessionParser parser = new SessionParser(
            session, sessionIdStrategy, authenticationStrategy, validationStrategy);
        final SessionHandler handler = configuration.newSessionHandler().onConnect(session);
        final SessionSubscriber subscriber = new SessionSubscriber(parser, session, handler,
            receiveTimer, sessionTimer);
        connectionIdToSession.put(connectionId, subscriber);
        sessions.add(session);
    }

    private Session initiateSession(final long connectionId,
                                    final int lastSequenceNumber,
                                    final int lastReceivedSequenceNumber)
    {
        final Object key = sessionIdStrategy.onInitiatorLogon(
            sessionConfiguration.senderCompId(), sessionConfiguration.senderSubId(),
            sessionConfiguration.senderLocationId(), sessionConfiguration.targetCompId());
        final int defaultInterval = configuration.defaultHeartbeatInterval();
        final GatewayPublication publication = outboundLibraryStreams.gatewayPublication(idleStrategy);

        return new InitiatorSession(
            defaultInterval,
            connectionId,
            clock,
            sessionProxy(connectionId).setupSession(-1, key),
            publication,
            sessionIdStrategy,
            configuration.beginString(),
            configuration.sendingTimeWindowInMs(),
            fixCounters.receivedMsgSeqNo(connectionId),
            fixCounters.sentMsgSeqNo(connectionId),
            sessionConfiguration.username(),
            sessionConfiguration.password(),
            libraryId,
            sessionConfiguration.bufferSize(),
            initiatorInitialSequenceNumber(lastSequenceNumber))
            .lastReceivedMsgSeqNum(initiatorInitialSequenceNumber(lastReceivedSequenceNumber) - 1);
    }

    private int initiatorInitialSequenceNumber(
        final int lastSequenceNumber)
    {
        if (sessionConfiguration.hasCustomInitialSequenceNumber())
        {
            return sessionConfiguration.initialSequenceNumber();
        }

        if (sessionConfiguration.sequenceNumbersPersistent() && lastSequenceNumber != SequenceNumberIndexReader.UNKNOWN_SESSION)
        {
            return lastSequenceNumber;
        }

        return 1;
    }

    private Session acceptSession(final long connectionId, final int lastSequenceNumber, final String address)
    {
        final GatewayPublication publication = outboundLibraryStreams.gatewayPublication(
            idleStrategy);
        final int defaultInterval = configuration.defaultHeartbeatInterval();
        final int split = address.lastIndexOf(':');
        final int start = address.startsWith("/") ? 1 : 0;
        final String host = address.substring(start, split);
        final int port = Integer.parseInt(address.substring(split + 1));

        return new AcceptorSession(
            defaultInterval,
            connectionId,
            clock,
            sessionProxy(connectionId),
            publication,
            sessionIdStrategy,
            configuration.beginString(),
            configuration.sendingTimeWindowInMs(),
            fixCounters.receivedMsgSeqNo(connectionId),
            fixCounters.sentMsgSeqNo(connectionId),
            libraryId,
            configuration.acceptorSessionBufferSize(),
            1)
            .address(host, port);
    }

    private SessionProxy sessionProxy(final long connectionId)
    {
        return new SessionProxy(
            configuration.encoderBufferSize(),
            outboundLibraryStreams.gatewayPublication(idleStrategy),
            sessionIdStrategy,
            configuration.sessionCustomisationStrategy(),
            new SystemEpochClock(),
            connectionId,
            libraryId);
    }
}
