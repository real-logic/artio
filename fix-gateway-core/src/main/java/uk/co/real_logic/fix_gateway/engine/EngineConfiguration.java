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
package uk.co.real_logic.fix_gateway.engine;

import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.CommonConfiguration;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Objects;

import static java.lang.Integer.getInteger;
import static java.lang.System.getProperty;

/**
 * Configuration that exists for the entire duration of a fix gateway. Some options are configurable via
 * commandline properties. Setters override commandline properties, not the other way around.
 * <p>
 * See setters or properties for documentation of what specific configuration options do.
 *
 * @see FixEngine
 */
public final class EngineConfiguration extends CommonConfiguration implements AutoCloseable
{
    // ------------------------------------------------
    //          Configuration Properties
    // ------------------------------------------------

    /** Property name for the directory to log archive data into */
    public static final String LOG_FILE_DIR_PROP = "logging.dir";
    /** Property name for size of logging index files */
    public static final String INDEX_FILE_SIZE_PROP = "logging.index.size";

    // Care needs to be taken when setting the fragment limits, and buffer sizes
    // The inbound bytes received and buffer sizes should always be set low enough
    // that fragments will be read off of the TCP connections slower
    // that they can be written onto them.

    /** Property name for the max number of messages to read from libraries.*/
    public static final String OUTBOUND_LIBRARY_FRAGMENT_LIMIT_PROP = "fix.core.outbound_fragment_limit";
    /** Property name for the max number of messages to read from replayer. */
    public static final String REPLAY_FRAGMENT_LIMIT_PROP = "fix.core.replay_fragment_limit";
    /** Property name for the max number of bytes to read from all TCP Connections. */
    public static final String INBOUND_BYTES_RECEIVED_LIMIT_PROP = "fix.core.inbound_bytes_limit";
    /** Property name for the size in bytes of the receiver end point's framing buffer. */
    public static final String RECEIVER_BUFFER_SIZE_PROP = "fix.core.receiver_buffer_size";
    /** Property name for the size in bytes of the TCP socket's receive buffer. */
    public static final String RECEIVER_SOCKET_BUFFER_SIZE_PROP = "fix.core.receiver_socket_buffer_size";
    /** Property name for the size in bytes of the TCP socket's send buffer. */
    public static final String SENDER_SOCKET_BUFFER_SIZE_PROP = "fix.core.sender_socket_buffer_size";
    /** Property name for the size in bytes of the sequence number cache file*/
    public static final String SEQUENCE_NUMBER_INDEX_SIZE_PROP = "fix.core.sequence_number_cache_size";
    /** Property name for the size in bytes of the session id file */
    public static final String SESSION_ID_BUFFER_SIZE_PROP = "fix.core.session_id_file_size";

    // ------------------------------------------------
    //          Configuration Defaults
    // ------------------------------------------------

    public static final String DEFAULT_LOG_FILE_DIR = "logs";
    public static final int DEFAULT_INDEX_FILE_SIZE = 2 * 1024 * 1024;
    public static final int DEFAULT_LOGGER_CACHE_NUM_SETS = 8;
    public static final int DEFAULT_LOGGER_CACHE_SET_SIZE = 4;

    public static final int DEFAULT_OUTBOUND_LIBRARY_FRAGMENT_LIMIT = 100;
    public static final int DEFAULT_REPLAY_FRAGMENT_LIMIT = 5;
    public static final int DEFAULT_INBOUND_BYTES_RECEIVED_LIMIT = 8 * 1024;
    public static final int DEFAULT_RECEIVER_BUFFER_SIZE = 8 * 1024;
    public static final int DEFAULT_RECEIVER_SOCKET_BUFFER_SIZE = 1024 * 1024;
    public static final int DEFAULT_SENDER_SOCKET_BUFFER_SIZE = 1024 * 1024;
    public static final int DEFAULT_SEQUENCE_NUMBER_INDEX_SIZE = 8 * 1024 * 1024;
    public static final int DEFAULT_SESSION_ID_BUFFER_SIZE = 4 * 1024 * 1024;

    private String host = null;
    private int port;
    private int indexFileSize = getInteger(INDEX_FILE_SIZE_PROP, DEFAULT_INDEX_FILE_SIZE);
    private String logFileDir = getProperty(LOG_FILE_DIR_PROP, DEFAULT_LOG_FILE_DIR);
    private int loggerCacheNumSets = DEFAULT_LOGGER_CACHE_NUM_SETS;
    private int loggerCacheSetSize = DEFAULT_LOGGER_CACHE_SET_SIZE;
    private boolean logInboundMessages = true;
    private boolean logOutboundMessages = true;
    private boolean printErrorMessages = true;
    private IdleStrategy framerIdleStrategy = backoffIdleStrategy();
    private IdleStrategy loggerIdleStrategy = backoffIdleStrategy();
    private IdleStrategy errorPrinterIdleStrategy = new BackoffIdleStrategy(1, 1, 1000, 1_000_000);
    private AtomicBuffer sentSequenceNumberBuffer;
    private AtomicBuffer receivedSequenceNumberBuffer;
    private MappedFile sentSequenceNumberIndex;
    private MappedFile receivedSequenceNumberIndex;
    private MappedFile sessionIdBuffer;

    private int outboundLibraryFragmentLimit =
        getInteger(OUTBOUND_LIBRARY_FRAGMENT_LIMIT_PROP, DEFAULT_OUTBOUND_LIBRARY_FRAGMENT_LIMIT);
    private int replayFragmentLimit =
        getInteger(REPLAY_FRAGMENT_LIMIT_PROP, DEFAULT_REPLAY_FRAGMENT_LIMIT);
    private int inboundBytesReceivedLimit =
        getInteger(INBOUND_BYTES_RECEIVED_LIMIT_PROP, DEFAULT_INBOUND_BYTES_RECEIVED_LIMIT);
    private int receiverBufferSize =
        getInteger(RECEIVER_BUFFER_SIZE_PROP, DEFAULT_RECEIVER_BUFFER_SIZE);
    private int receiverSocketBufferSize =
        getInteger(RECEIVER_SOCKET_BUFFER_SIZE_PROP, DEFAULT_RECEIVER_SOCKET_BUFFER_SIZE);
    private int senderSocketBufferSize =
        getInteger(SENDER_SOCKET_BUFFER_SIZE_PROP, DEFAULT_SENDER_SOCKET_BUFFER_SIZE);
    private int sequenceNumberIndexSize =
        getInteger(SEQUENCE_NUMBER_INDEX_SIZE_PROP, DEFAULT_SEQUENCE_NUMBER_INDEX_SIZE);
    private int sessionIdBufferSize =
        getInteger(SESSION_ID_BUFFER_SIZE_PROP, DEFAULT_SESSION_ID_BUFFER_SIZE);

    /**
     * Sets the local address to bind to when the Gateway is used to accept connections.
     * <p>
     * Optional.
     *
     * @param host the hostname to bind to.
     * @param port the port to bind to.
     * @return this
     */
    public EngineConfiguration bindTo(final String host, final int port)
    {
        Objects.requireNonNull(host, "host");
        this.host = host;
        this.port = port;
        return this;
    }

    /**
     * Sets the receiver buffer size.
     *
     * @param receiverBufferSize the receiver buffer size.
     * @return this
     *
     * @see EngineConfiguration#RECEIVER_BUFFER_SIZE_PROP
     */
    public EngineConfiguration receiverBufferSize(final int receiverBufferSize)
    {
        this.receiverBufferSize = receiverBufferSize;
        return this;
    }

    /**
     * Sets the receiver socket buffer size.
     *
     * @param receiverSocketBufferSize the receiver socket buffer size.
     * @return this
     *
     * @see EngineConfiguration#RECEIVER_SOCKET_BUFFER_SIZE_PROP
     */
    public EngineConfiguration receiverSocketBufferSize(final int receiverSocketBufferSize)
    {
        this.receiverSocketBufferSize = receiverSocketBufferSize;
        return this;
    }

    /**
     * Sets the sender socket buffer size.
     *
     * @param senderSocketBufferSize the receiver socket buffer size.
     * @return this
     *
     * @see EngineConfiguration#SENDER_SOCKET_BUFFER_SIZE_PROP
     */
    public EngineConfiguration senderSocketBufferSize(final int senderSocketBufferSize)
    {
        this.senderSocketBufferSize = senderSocketBufferSize;
        return this;
    }

    /**
     * Sets the directory to store log files in.
     *
     * @param logFileDir the directory to store log files in.
     * @return this
     *
     * @see EngineConfiguration#LOG_FILE_DIR_PROP
     */
    public EngineConfiguration logFileDir(final String logFileDir)
    {
        this.logFileDir = logFileDir;
        return this;
    }

    /**
     * Sets the size of index files.
     *
     * @param indexFileSize the size of index files.
     * @return this
     *
     * @see EngineConfiguration#INDEX_FILE_SIZE_PROP
     */
    public EngineConfiguration indexFileSize(final int indexFileSize)
    {
        this.indexFileSize = indexFileSize;
        return this;
    }

    /**
     * Sets the set size of the logger's caches.
     * <p>
     * The logging and archival mechanism
     * has several caches of open memory mapped files which it stores streams of messages
     * into. This and {@link this#loggerCacheNumSets} controls the size of those caches.
     * Should be increased if you see files being opened/closed in that area too frequently.
     * <p>
     * {@link uk.co.real_logic.agrona.collections.Int2ObjectCache} explains the difference between set size
     * and num sets.
     *
     * @param loggerCacheSetSize the set size of the logger's caches.
     * @return this
     */
    public EngineConfiguration loggerCacheSetSize(final int loggerCacheSetSize)
    {
        this.loggerCacheSetSize = loggerCacheSetSize;
        return this;
    }

    /**
     * Sets the number of sets of in the logger's caches.
     *
     * @param loggerCacheNumSets the number of sets of in the logger's caches.
     * @return this
     *
     * @see this#loggerCacheSetSize(int)
     */
    public EngineConfiguration loggerCacheNumSets(final int loggerCacheNumSets)
    {
        this.loggerCacheNumSets = loggerCacheNumSets;
        return this;
    }

    /**
     * Sets logging of inbound messages.
     * <p>
     * Switch off if you don't want the logging system to store all inbound messages in the archival system.
     * <p>
     * Default: true.
     *
     * @param logInboundMessages logging of inbound messages.
     * @return this
     */
    public EngineConfiguration logInboundMessages(final boolean logInboundMessages)
    {
        this.logInboundMessages = logInboundMessages;
        return this;
    }

    /**
     * Sets logging of outbound messages.
     * <p>
     * Switch off if you don't want the logging system to store all outbound messages.
     * <b>NB:</b> take care if you switch this off as message replay won't work.
     * <p>
     * Default: true.
     *
     * @param logOutboundMessages logging of outbound messages.
     * @return this
     */
    public EngineConfiguration logOutboundMessages(final boolean logOutboundMessages)
    {
        this.logOutboundMessages = logOutboundMessages;
        return this;
    }

    /**
     * Sets the printing of error messages on or off. Error messages are always logged in an error buffer that
     * can be scanned by another diagnostic process, this simply switches on or off the printing these errors on
     * standard out.
     * <p>
     * Default: true
     *
     * @param printErrorMessages the printing of error messages.
     * @return this
     */
    public EngineConfiguration printErrorMessages(final boolean printErrorMessages)
    {
        this.printErrorMessages = printErrorMessages;
        return this;
    }

    /**
     * Sets the idle strategy for the Framer thread.
     *
     * @param framerIdleStrategy the idle strategy for the Framer thread.
     * @return this
     */
    public EngineConfiguration framerIdleStrategy(final IdleStrategy framerIdleStrategy)
    {
        this.framerIdleStrategy = framerIdleStrategy;
        return this;
    }

    /**
     * Sets the idle strategy for the Logger thread.
     *
     * @param loggerIdleStrategy the idle strategy for the Logger thread.
     * @return this
     */
    public EngineConfiguration loggerIdleStrategy(final IdleStrategy loggerIdleStrategy)
    {
        this.loggerIdleStrategy = loggerIdleStrategy;
        return this;
    }

    /**
     * Sets the idle strategy for the Error Printer thread.
     *
     * @param errorPrinterIdleStrategy the idle strategy for the Error Printer thread.
     * @return this
     */
    public EngineConfiguration errorPrinterIdleStrategy(final IdleStrategy errorPrinterIdleStrategy)
    {
        this.errorPrinterIdleStrategy = errorPrinterIdleStrategy;
        return this;
    }

    /**
     * Sets the fragment limit for the subscription to outbound messages from libraries.
     *
     * @param outboundLibraryFragmentLimit the fragment limit for the subscription to outbound messages from libraries.
     * @return this
     *
     * @see EngineConfiguration#OUTBOUND_LIBRARY_FRAGMENT_LIMIT_PROP
     */
    public EngineConfiguration outboundLibraryFragmentLimit(final int outboundLibraryFragmentLimit)
    {
        this.outboundLibraryFragmentLimit = outboundLibraryFragmentLimit;
        return this;
    }

    /**
     * Sets the fragment limit for the subscription to messages from the replayer.
     *
     * @param outboundReplayFragmentLimit the fragment limit for the subscription to messages from the replayer.
     * @return this
     *
     * @see EngineConfiguration#REPLAY_FRAGMENT_LIMIT_PROP
     */
    public EngineConfiguration replayFragmentLimit(final int outboundReplayFragmentLimit)
    {
        this.replayFragmentLimit = outboundReplayFragmentLimit;
        return this;
    }

    /**
     * Sets the bytes limit for receiving inbound messages.
     *
     * @param inboundBytesReceivedLimit the bytes limit for receiving inbound messages.
     * @return this
     *
     * @see EngineConfiguration#INBOUND_BYTES_RECEIVED_LIMIT_PROP
     */
    public EngineConfiguration inboundBytesReceivedLimit(final int inboundBytesReceivedLimit)
    {
        this.inboundBytesReceivedLimit = inboundBytesReceivedLimit;
        return this;
    }

    public int receiverBufferSize()
    {
        return receiverBufferSize;
    }

    public int receiverSocketBufferSize()
    {
        return receiverSocketBufferSize;
    }

    public int senderSocketBufferSize()
    {
        return senderSocketBufferSize;
    }

    public boolean hasBindAddress()
    {
        return host != null;
    }

    public InetSocketAddress bindAddress()
    {
        return new InetSocketAddress(host, port);
    }

    public String logFileDir()
    {
        return logFileDir;
    }

    public int indexFileSize()
    {
        return indexFileSize;
    }

    public int loggerCacheSetSize()
    {
        return loggerCacheSetSize;
    }

    public int loggerCacheNumSets()
    {
        return loggerCacheNumSets;
    }

    public boolean logInboundMessages()
    {
        return logInboundMessages;
    }

    public boolean logOutboundMessages()
    {
        return logOutboundMessages;
    }

    public boolean printErrorMessages()
    {
        return printErrorMessages;
    }

    public IdleStrategy framerIdleStrategy()
    {
        return framerIdleStrategy;
    }

    public IdleStrategy errorPrinterIdleStrategy()
    {
        return errorPrinterIdleStrategy;
    }

    public IdleStrategy loggerIdleStrategy()
    {
        return loggerIdleStrategy;
    }

    public int outboundLibraryFragmentLimit()
    {
        return outboundLibraryFragmentLimit;
    }

    public int replayFragmentLimit()
    {
        return replayFragmentLimit;
    }

    public int inboundBytesReceivedLimit()
    {
        return inboundBytesReceivedLimit;
    }

    public MappedFile sentSequenceNumberIndex()
    {
        return sentSequenceNumberIndex;
    }

    public AtomicBuffer sentSequenceNumberBuffer()
    {
        return sentSequenceNumberBuffer;
    }

    public MappedFile receivedSequenceNumberIndex()
    {
        return receivedSequenceNumberIndex;
    }

    public AtomicBuffer receivedSequenceNumberBuffer()
    {
        return receivedSequenceNumberBuffer;
    }

    public MappedFile sessionIdBuffer()
    {
        return sessionIdBuffer;
    }

    /**
     * {@inheritDoc}
     */
    public EngineConfiguration aeronChannel(final String aeronChannel)
    {
        super.aeronChannel(aeronChannel);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public EngineConfiguration monitoringBuffersLength(final Integer monitoringBuffersLength)
    {
        super.monitoringBuffersLength(monitoringBuffersLength);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public EngineConfiguration monitoringFile(String monitoringFile)
    {
        super.monitoringFile(monitoringFile);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public EngineConfiguration replyTimeoutInMs(final long replyTimeoutInMs)
    {
        super.replyTimeoutInMs(replyTimeoutInMs);
        return this;
    }

    void conclude()
    {
        super.conclude("engine");

        if (sentSequenceNumberIndex() == null)
        {
            sentSequenceNumberIndex = mapFile("sequence_numbers_sent", sequenceNumberIndexSize);
        }

        if (sentSequenceNumberBuffer() == null)
        {
            sentSequenceNumberBuffer = new UnsafeBuffer(new byte[sequenceNumberIndexSize]);
        }

        if (receivedSequenceNumberIndex() == null)
        {
            receivedSequenceNumberIndex = mapFile("sequence_numbers_received", sequenceNumberIndexSize);
        }

        if (receivedSequenceNumberBuffer() == null)
        {
            receivedSequenceNumberBuffer = new UnsafeBuffer(new byte[sequenceNumberIndexSize]);
        }

        if (sessionIdBuffer() == null)
        {
            sessionIdBuffer = mapFile("session_id_buffer", sessionIdBufferSize);
        }
    }

    private MappedFile mapFile(final String file, final int size)
    {
        return MappedFile.map(logFileDir() + File.separator + file, size);
    }

    public void close()
    {
        CloseHelper.close(sentSequenceNumberIndex());
        CloseHelper.close(receivedSequenceNumberIndex());
        CloseHelper.close(sessionIdBuffer());
    }
}
