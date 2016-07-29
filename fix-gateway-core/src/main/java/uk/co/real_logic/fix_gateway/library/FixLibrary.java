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

import uk.co.real_logic.fix_gateway.FixGatewayException;
import uk.co.real_logic.fix_gateway.GatewayProcess;
import uk.co.real_logic.fix_gateway.messages.SessionReplyStatus;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.timing.LibraryTimers;

import java.util.List;

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
    public static final int NO_MESSAGE_REPLAY = -1;

    private final LibraryPoller poller;

    private FixLibrary(final LibraryConfiguration configuration)
    {
        init(configuration);
        final LibraryTimers timers = new LibraryTimers();
        initMonitoringAgent(timers.all(), configuration);

        final LibraryTransport transport = new LibraryTransport(
            configuration, fixCounters, aeron);
        poller = new LibraryPoller(
            configuration, timers, fixCounters, transport, this);
    }

    private FixLibrary connect()
    {
        poller.connect();
        start();
        return this;
    }

    // ------------- Public API -------------

    /**
     * Connect to an engine. This method blocks until the connection is complete and then returns.
     *
     * @param configuration the configuration for this library instance.
     * @return the library instance once it has connected.
     * @throws FixGatewayException
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
        return poller.poll(fragmentLimit);
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
        return poller.isConnected();
    }

    /**
     * Get the identifier of the library.
     *
     * @return the identifier of the library.
     */
    public int libraryId()
    {
        return poller.libraryId();
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
        return poller.sessions();
    }

    /**
     * Close the Library.
     */
    public void close()
    {
        poller.close();
        super.close();
    }

    /**
     * Initiate a FIX session with a FIX acceptor. This method returns a reply object
     * wrapping the Session itself.
     *
     * @param configuration the configuration to use for the session.
     * @return the session object for the session that you've initiated. It can return the following errors:
     *         {@link IllegalStateException}
     *         if you're trying to initiate two sessions at the same time or if there's a timeout talking to
     *         the {@link uk.co.real_logic.fix_gateway.engine.FixEngine}.
     *         This probably indicates that there's a problem in your code or that your engine isn't running.
     *         {@link FixGatewayException}
     *         if you're unable to connect to the accepting gateway.
     *         This probably indicates a configuration problem related to the external gateway.
     */
    public Reply<Session> initiate(final SessionConfiguration configuration)
    {
        return poller.initiate(configuration);
    }

    /**
     * Release this session object to the gateway to manage. If the release
     * operation has successfully completed then it will return {@link SessionReplyStatus#OK}.
     *
     * Similar to {@link this#initiate(SessionConfiguration)} this is a non-blocking operation that
     * returns a reply object that indicates what has happened to its result.
     *
     * @param session the session to release
     * @return the result of this operation.
     */
    public Reply<SessionReplyStatus> releaseToGateway(final Session session)
    {
        return poller.releaseToGateway(session);
    }

    /**
     * Request a session be acquired from the Gateway. It returns a {@link Reply} object.
     *
     * If this session is being managed by
     * the gateway then your {@link SessionAcquireHandler} will receive a callback
     * and the reply will be {@link SessionReplyStatus#OK}.
     *
     * If another library has acquired the session then this method will return
     * {@link SessionReplyStatus#OTHER_SESSION_OWNER}. If the connection id refers
     * to an unknown session then the method returns {@link SessionReplyStatus#UNKNOWN_SESSION}.
     * If this library instance is unknown to the gateway, for example if its heartbeating
     * mechanism has timed out due to {@link this#poll(int)} not being called often enough.
     *
     * @param sessionId the id of the session to acquire.
     * @param lastReceivedSequenceNumber the last received message sequence number
     *                                   that you know about. You will get a stream
     *                                   of messages replayed to you from
     *                                   <code>lastReceivedMessageSequenceNumber + 1</code>
     *                                   to the latest message sequence number.
     *                                   If you don't care about message replay then
     *                                   use {@link FixLibrary#NO_MESSAGE_REPLAY} as the parameter.
     * @return the reply object representing the result of the request.
     */
    public Reply<SessionReplyStatus> requestSession(final long sessionId, final int lastReceivedSequenceNumber)
    {
        return poller.requestSession(sessionId, lastReceivedSequenceNumber);
    }

    public String currentAeronChannel()
    {
        return poller.currentAeronChannel();
    }
}
