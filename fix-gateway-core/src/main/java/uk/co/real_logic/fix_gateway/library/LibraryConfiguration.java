/*
 * Copyright 2015 Real Logic Ltd.
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

import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.CommonConfiguration;
import uk.co.real_logic.fix_gateway.library.session.NewSessionHandler;
import uk.co.real_logic.fix_gateway.library.session.NoSessionCustomisationStrategy;
import uk.co.real_logic.fix_gateway.library.session.SessionCustomisationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.MessageValidationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.NoAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.NoMessageValidationStrategy;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import static java.util.concurrent.TimeUnit.MINUTES;
import static uk.co.real_logic.fix_gateway.library.SessionConfiguration.DEFAULT_SESSION_BUFFER_SIZE;


/**
 * Provides configuration for initiating an instance of Fix Library. Individual configuration options are
 * documented on their setters.
 *
 * @see FixLibrary
 */
public final class LibraryConfiguration extends CommonConfiguration
{

    public static final String DEFAULT_BEGIN_STRING = "FIX.4.4";
    public static final int DEFAULT_HEARTBEAT_INTERVAL = 10;
    public static final int DEFAULT_ENCODER_BUFFER_SIZE = 8 * 1024;
    public static final long DEFAULT_SENDING_TIME_WINDOW = MINUTES.toMillis(2);
    public static final int DEFAULT_LIBRARY_ID = 1;

    private int defaultHeartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private int encoderBufferSize = DEFAULT_ENCODER_BUFFER_SIZE;
    private NewSessionHandler newSessionHandler;
    private char[] beginString;
    private long sendingTimeWindowInMs = DEFAULT_SENDING_TIME_WINDOW;
    private AuthenticationStrategy authenticationStrategy = new NoAuthenticationStrategy();
    private MessageValidationStrategy messageValidationStrategy = new NoMessageValidationStrategy();
    private SessionCustomisationStrategy sessionCustomisationStrategy = new NoSessionCustomisationStrategy();
    private int libraryId = DEFAULT_LIBRARY_ID;
    private int acceptorSessionBufferSize = DEFAULT_SESSION_BUFFER_SIZE;
    private IdleStrategy libraryIdleStrategy = backoffIdleStrategy();
    private boolean isAcceptor = false;
    private boolean acceptorSequenceNumbersResetUponReconnect;

    public LibraryConfiguration()
    {
        beginString(DEFAULT_BEGIN_STRING);
    }

    /**
     * The default interval for heartbeats if not exchanged upon logon. Specified in seconds.
     *
     * @return this
     */
    public LibraryConfiguration defaultHeartbeatInterval(final int value)
    {
        defaultHeartbeatInterval = value;
        return this;
    }

    /**
     * When a new session connects to the gateway you register a callback handler to find
     * out about the event. This method sets the handler for this library instance.
     * <p>
     * Only needed if this is the accepting library instance.
     *
     * @param newSessionHandler the new session handler
     * @return this
     */
    public LibraryConfiguration newSessionHandler(final NewSessionHandler newSessionHandler)
    {
        this.newSessionHandler = newSessionHandler;
        return this;
    }

    /**
     * The begin string to use for messages in the FIX protocol. This is used for validating
     * messages received by the gateway.
     *
     * @param beginString the beginString to use.
     * @return this
     */
    public LibraryConfiguration beginString(final String beginString)
    {
        this.beginString = beginString.toCharArray();
        return this;
    }

    /**
     * Sets the sending time window. The sending time window is the period of acceptance
     * delta between the current time on the Fix Library thread and the sending time
     * received in messages. Sessions are disconnected if the sending time diverges by
     * more than this window and if validation is enabled.
     *
     * @param sendingTimeWindowInMs the current sending time in milliseconds
     * @return this
     */
    public LibraryConfiguration sendingTimeWindowInMs(long sendingTimeWindowInMs)
    {
        this.sendingTimeWindowInMs = sendingTimeWindowInMs;
        return this;
    }

    /**
     * Sets size of the encoder buffer. The encoder buffer is a buffer used by each session to encode
     * FIX messages onto before they're sent over a FIX connection.
     *
     * @param encoderBufferSize size of the encoder buffer in bytes.
     * @return this
     */
    public LibraryConfiguration encoderBufferSize(final int encoderBufferSize)
    {
        this.encoderBufferSize = encoderBufferSize;
        return this;
    }

    /**
     * Sets the authentication strategy of the FIX Library, see {@link AuthenticationStrategy} for details.
     * <p>
     * This only needs to be set if this FIX Library is the acceptor library.
     *
     * @param authenticationStrategy the authentication strategy to use.
     * @return this
     */
    public LibraryConfiguration authenticationStrategy(final AuthenticationStrategy authenticationStrategy)
    {
        this.authenticationStrategy = authenticationStrategy;
        return this;
    }

    /**
     * Sets the session customisation strategy of the FIX Library,
     * see {@link SessionCustomisationStrategy} for details.
     * <p>
     * This only needs to be set if this FIX Library is the acceptor library.
     *
     * @param sessionCustomisationStrategy the session customisation strategy to use.
     * @return this
     */
    public LibraryConfiguration sessionCustomisationStrategy(
        final SessionCustomisationStrategy sessionCustomisationStrategy)
    {
        this.sessionCustomisationStrategy = sessionCustomisationStrategy;
        return this;
    }

    /**
     * Sets the message validation strategy of the FIX Library,
     * see {@link MessageValidationStrategy} for details.
     *
     * @param messageValidationStrategy the message validation strategy to use.
     * @return this
     */
    public LibraryConfiguration messageValidationStrategy(final MessageValidationStrategy messageValidationStrategy)
    {
        this.messageValidationStrategy = messageValidationStrategy;
        return this;
    }

    /**
     * Sets the identifier for this library instance. The identifier should be unique amongst the libraries that
     * are connected to this gateway.
     *
     * @param libraryId the identifier for this library instance
     * @return this
     */
    public LibraryConfiguration libraryId(final int libraryId)
    {
        this.libraryId = libraryId;
        return this;
    }

    /**
     * Sets the size of an acceptor's session buffer. The session buffer is a buffer used by each
     * Session to encode messages via {@link uk.co.real_logic.fix_gateway.library.session.Session#send(uk.co.real_logic.fix_gateway.builder.MessageEncoder)}.
     * <p>
     * This only needs to be set if this FIX Library is the acceptor library.
     *
     * @param acceptorSessionBufferSize the size of an acceptor's session buffer
     * @return this
     */
    public LibraryConfiguration acceptorSessionBufferSize(final int acceptorSessionBufferSize)
    {
        this.acceptorSessionBufferSize = acceptorSessionBufferSize;
        return this;
    }

    /**
     * Sets the idle strategy for the FIX library instance.
     *
     * @param libraryIdleStrategy the idle strategy for the FIX library instance.
     * @return this
     */
    public LibraryConfiguration libraryIdleStrategy(final IdleStrategy libraryIdleStrategy)
    {
        this.libraryIdleStrategy = libraryIdleStrategy;
        return this;
    }

    /**
     * Sets whether this FIX library instance is the acceptor instance. When new connections arrive at a FIX
     * gateway they need to be forwarded to an instance called the the acceptor library. Only one library instance
     * per gateway can be the acceptor.
     *
     * @param isAcceptor whether this FIX library instance is the acceptor instance.
     * @return this
     */
    public LibraryConfiguration isAcceptor(final boolean isAcceptor)
    {
        this.isAcceptor = isAcceptor;
        return this;
    }

    /**
     * Configure whether you want the session to reset its sequence number when it reconnects.
     * The session is determined to be the same if the session id strategy allocates it the same
     * id.
     *
     * @param value true if you want them to reset
     * @return this configuration object.
     *
     * @see SessionConfiguration#sequenceNumbersPersistent()
     * @see this#sessionIdStrategy(SessionIdStrategy)
     */
    public LibraryConfiguration acceptorSequenceNumbersResetUponReconnect(final boolean value)
    {
        this.acceptorSequenceNumbersResetUponReconnect = value;
        return this;
    }

    public int defaultHeartbeatInterval()
    {
        return defaultHeartbeatInterval;
    }

    public int encoderBufferSize()
    {
        return encoderBufferSize;
    }

    public AuthenticationStrategy authenticationStrategy()
    {
        return authenticationStrategy;
    }

    public NewSessionHandler newSessionHandler()
    {
        return newSessionHandler;
    }

    public char[] beginString()
    {
        return beginString;
    }

    public long sendingTimeWindowInMs()
    {
        return sendingTimeWindowInMs;
    }

    public SessionCustomisationStrategy sessionCustomisationStrategy()
    {
        return sessionCustomisationStrategy;
    }

    public MessageValidationStrategy messageValidationStrategy()
    {
        return messageValidationStrategy;
    }

    public int libraryId()
    {
        return libraryId;
    }

    public int acceptorSessionBufferSize()
    {
        return acceptorSessionBufferSize;
    }

    public IdleStrategy libraryIdleStrategy()
    {
        return libraryIdleStrategy;
    }

    public boolean isAcceptor()
    {
        return isAcceptor;
    }

    public boolean acceptorSequenceNumbersResetUponReconnect()
    {
        return acceptorSequenceNumbersResetUponReconnect;
    }

    /**
     * {@inheritDoc}
     */
    public LibraryConfiguration sessionIdStrategy(final SessionIdStrategy sessionIdStrategy)
    {
        super.sessionIdStrategy(sessionIdStrategy);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public LibraryConfiguration counterBuffersLength(final Integer counterBuffersLength)
    {
        super.counterBuffersLength(counterBuffersLength);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public LibraryConfiguration monitoringFile(String monitoringFile)
    {
        super.monitoringFile(monitoringFile);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public LibraryConfiguration aeronChannel(final String aeronChannel)
    {
        super.aeronChannel(aeronChannel);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public LibraryConfiguration replyTimeoutInMs(final long replyTimeoutInMs)
    {
        super.replyTimeoutInMs(replyTimeoutInMs);
        return this;
    }

    void conclude()
    {
        super.conclude("library-" + libraryId());
    }

}
