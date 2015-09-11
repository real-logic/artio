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
    private long sendingTimeWindow = DEFAULT_SENDING_TIME_WINDOW;
    private AuthenticationStrategy authenticationStrategy = new NoAuthenticationStrategy();
    private MessageValidationStrategy messageValidationStrategy = new NoMessageValidationStrategy();
    private SessionCustomisationStrategy sessionCustomisationStrategy = new NoSessionCustomisationStrategy();
    private int libraryId = DEFAULT_LIBRARY_ID;
    private int acceptorSessionBufferSize = DEFAULT_SESSION_BUFFER_SIZE;
    private IdleStrategy libraryIdleStrategy = backoffIdleStrategy();
    private boolean isAcceptor = false;

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

    public LibraryConfiguration newSessionHandler(final NewSessionHandler newSessionHandler)
    {
        this.newSessionHandler = newSessionHandler;
        return this;
    }

    public LibraryConfiguration beginString(final String beginString)
    {
        this.beginString = beginString.toCharArray();
        return this;
    }

    public LibraryConfiguration sendingTimeWindow(long sendingTimeWindow)
    {
        this.sendingTimeWindow = sendingTimeWindow;
        return this;
    }

    public LibraryConfiguration encoderBufferSize(final int encoderBufferSize)
    {
        this.encoderBufferSize = encoderBufferSize;
        return this;
    }


    public LibraryConfiguration authenticationStrategy(final AuthenticationStrategy authenticationStrategy)
    {
        this.authenticationStrategy = authenticationStrategy;
        return this;
    }

    public LibraryConfiguration sessionCustomisationStrategy(final SessionCustomisationStrategy value)
    {
        this.sessionCustomisationStrategy = value;
        return this;
    }

    public LibraryConfiguration messageValidationStrategy(final MessageValidationStrategy messageValidationStrategy)
    {
        this.messageValidationStrategy = messageValidationStrategy;
        return this;
    }

    public LibraryConfiguration libraryId(final int libraryId)
    {
        this.libraryId = libraryId;
        return this;
    }

    public LibraryConfiguration acceptorSessionBufferSize(final int acceptorSessionBufferSize)
    {
        this.acceptorSessionBufferSize = acceptorSessionBufferSize;
        return this;
    }

    public LibraryConfiguration libraryIdleStrategy(final IdleStrategy libraryIdleStrategy)
    {
        this.libraryIdleStrategy = libraryIdleStrategy;
        return this;
    }

    public LibraryConfiguration isAcceptor(final boolean isAcceptor)
    {
        this.isAcceptor = isAcceptor;
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

    public long sendingTimeWindow()
    {
        return sendingTimeWindow;
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

    public LibraryConfiguration sessionIdStrategy(final SessionIdStrategy sessionIdStrategy)
    {
        super.sessionIdStrategy(sessionIdStrategy);
        return this;
    }

    public LibraryConfiguration counterBuffersLength(final Integer counterBuffersLength)
    {
        super.counterBuffersLength(counterBuffersLength);
        return this;
    }

    public LibraryConfiguration monitoringFile(String monitoringFile)
    {
        super.monitoringFile(monitoringFile);
        return this;
    }

    public LibraryConfiguration aeronChannel(final String aeronChannel)
    {
        super.aeronChannel(aeronChannel);
        return this;
    }

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
