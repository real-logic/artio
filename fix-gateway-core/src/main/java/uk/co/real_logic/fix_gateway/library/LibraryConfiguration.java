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

import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.CommonConfiguration;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;


/**
 * Provides configuration for initiating an instance of Fix Library. Individual configuration options are
 * documented on their setters.
 *
 * @see FixLibrary
 */
public final class LibraryConfiguration extends CommonConfiguration
{

    public static final int DEFAULT_ENCODER_BUFFER_SIZE = 8 * 1024;
    public static final int DEFAULT_LIBRARY_ID = 1;

    private int encoderBufferSize = DEFAULT_ENCODER_BUFFER_SIZE;
    private NewSessionHandler newSessionHandler;

    private int libraryId = DEFAULT_LIBRARY_ID;
    private IdleStrategy libraryIdleStrategy = backoffIdleStrategy();
    private NewConnectHandler newConnectHandler;

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
     * Sets the identifier for this library instance. The identifier should be unique amongst the libraries that
     * are connected to this gateway.
     *
     * @param libraryId the identifier for this library instance
     * @return this
     */
    public LibraryConfiguration libraryId(final int libraryId)
    {
        if (libraryId < DEFAULT_LIBRARY_ID)
        {
            throw new IllegalArgumentException(
                String.format("Your Library Id was %d, Ids below %d are reserved.", libraryId, DEFAULT_LIBRARY_ID));
        }
        this.libraryId = libraryId;
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

    public LibraryConfiguration newConnectHandler(final NewConnectHandler newConnectHandler)
    {
        this.newConnectHandler = newConnectHandler;
        return this;
    }

    public int encoderBufferSize()
    {
        return encoderBufferSize;
    }

    public NewSessionHandler newSessionHandler()
    {
        return newSessionHandler;
    }

    public int libraryId()
    {
        return libraryId;
    }

    public IdleStrategy libraryIdleStrategy()
    {
        return libraryIdleStrategy;
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
    public LibraryConfiguration monitoringBuffersLength(final Integer monitoringBuffersLength)
    {
        super.monitoringBuffersLength(monitoringBuffersLength);
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

    NewConnectHandler newConnectHandler()
    {
        return newConnectHandler;
    }
}
