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
package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.fix_gateway.flyweight_api.OrderSingleAcceptor;
import uk.co.real_logic.fix_gateway.library.auth.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.auth.NoAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.session.NewSessionHandler;
import uk.co.real_logic.fix_gateway.library.session.NoSessionCustomisationStrategy;
import uk.co.real_logic.fix_gateway.library.session.SessionCustomisationStrategy;
import uk.co.real_logic.fix_gateway.otf.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.session.SenderAndTargetSessionIdStrategy;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.stream.IntStream;

import static java.lang.Integer.getInteger;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Configuration that exists for the entire duration of a fix gateway
 */
public final class StaticConfiguration
{
    // ------------------------------------------------
    //          Configuration Properties
    // ------------------------------------------------

    /** Property name for the flag to enable or disable debug logging */
    public static final String DEBUG_PRINT_MESSAGES_PROPERTY = "fix.core.debug";
    /** Property name for the file to log debug messages to, default is standard output */
    public static final String DEBUG_FILE_PROPERTY = "fix.core.debug.file";
    /** Property name for length of the memory mapped buffers for the counters file */
    public static final String COUNTER_BUFFERS_LENGTH_PROP_NAME = "fix.counters.length";
    /** Property name for directory of the conductor buffers */
    public static final String COUNTERS_FILE_PROP_NAME = "fix.counters.file";
    /** Property name for the directory to log archive data into */
    public static final String LOG_FILE_DIR_PROP = "logging.dir";
    /** Property name for size of logging index files */
    public static final String INDEX_FILE_SIZE_PROP = "logging.index.size";

    // ------------------------------------------------
    //          Configuration Defaults
    // ------------------------------------------------

    public static final int DEFAULT_HEARTBEAT_INTERVAL = 10;
    public static final int DEFAULT_RECEIVER_BUFFER_SIZE = 8 * 1024;
    public static final long DEFAULT_CONNECTION_TIMEOUT = 1000;
    public static final int DEFAULT_ENCODER_BUFFER_SIZE = 8 * 1024;
    public static final int DEFAULT_COUNTERS_BUFFER_LENGTH = 8 * 1024 * 1024;
    public static final int DEFAULT_INDEX_FILE_SIZE = 2 * 1024 * 1024;
    public static final String DEFAULT_COUNTERS_FILE_PROP = IoUtil.tmpDirName() + "fix" + File.separator + "counters";
    public static final String DEFAULT_LOG_FILE_DIR = "logs";
    public static final int DEFAULT_LOGGER_CACHE_CAPACITY = 10;
    public static final long DEFAULT_SENDING_TIME_WINDOW = MINUTES.toMillis(2);
    public static final String DEFAULT_BEGIN_STRING = "FIX.4.4";

    /** This is static final field in order to give the optimiser scope to remove references to it. */
    public static final boolean DEBUG_PRINT_MESSAGES = Boolean.getBoolean(DEBUG_PRINT_MESSAGES_PROPERTY);
    public static final String DEBUG_FILE = System.getProperty(DEBUG_FILE_PROPERTY);

    private final Int2ObjectHashMap<OtfMessageAcceptor> otfAcceptors = new Int2ObjectHashMap<>();

    private int defaultHeartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private int receiverBufferSize = DEFAULT_RECEIVER_BUFFER_SIZE;
    private int receiverSocketBufferSize = 0;
    private int senderSocketBufferSize = 0;
    private long connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
    private int encoderBufferSize = DEFAULT_ENCODER_BUFFER_SIZE;
    private SessionIdStrategy sessionIdStrategy = new SenderAndTargetSessionIdStrategy();
    private char[] beginString;

    private String host;
    private int port;
    private int indexFileSize = getInteger(INDEX_FILE_SIZE_PROP, DEFAULT_INDEX_FILE_SIZE);
    private String logFileDir = getProperty(LOG_FILE_DIR_PROP, DEFAULT_LOG_FILE_DIR);
    private int counterBuffersLength = getInteger(COUNTER_BUFFERS_LENGTH_PROP_NAME, DEFAULT_COUNTERS_BUFFER_LENGTH);
    private String counterBuffersFile = getProperty(COUNTERS_FILE_PROP_NAME, DEFAULT_COUNTERS_FILE_PROP);
    private String aeronChannel;
    private AuthenticationStrategy authenticationStrategy = new NoAuthenticationStrategy();
    private NewSessionHandler newSessionHandler;
    private int loggerCacheCapacity = DEFAULT_LOGGER_CACHE_CAPACITY;
    private long sendingTimeWindow = DEFAULT_SENDING_TIME_WINDOW;
    private SessionCustomisationStrategy sessionCustomisationStrategy = new NoSessionCustomisationStrategy();
    private boolean logMessages = true;

    public StaticConfiguration()
    {
        beginString(DEFAULT_BEGIN_STRING);
    }

    public void registerAcceptor(final OrderSingleAcceptor orderSingleAcceptor, final ErrorAcceptor errorAcceptor)
    {
    }

    public StaticConfiguration registerAcceptor(
        final OtfMessageAcceptor messageAcceptor, int firstTag, final int... tags)
    {
        otfAcceptors.put(firstTag, messageAcceptor);
        IntStream.of(tags).forEach(tag -> otfAcceptors.put(tag, messageAcceptor));
        return this;
    }

    public StaticConfiguration bind(final String host, final int port)
    {
        this.host = host;
        this.port = port;
        return this;
    }

    /**
     * The default interval for heartbeats if not exchanged upon logon. Specified in seconds.
     *
     * @return this
     */
    public StaticConfiguration defaultHeartbeatInterval(final int value)
    {
        defaultHeartbeatInterval = value;
        return this;
    }

    public StaticConfiguration receiverBufferSize(final int value)
    {
        receiverBufferSize = value;
        return this;
    }

    public StaticConfiguration receiverSocketBufferSize(final int value)
    {
        receiverSocketBufferSize = value;
        return this;
    }

    public StaticConfiguration senderSocketBufferSize(final int value)
    {
        senderSocketBufferSize = value;
        return this;
    }

    public StaticConfiguration sessionIdStrategy(final SessionIdStrategy sessionIdStrategy)
    {
        this.sessionIdStrategy = sessionIdStrategy;
        return this;
    }

    public StaticConfiguration counterBuffersLength(final Integer counterBuffersLength)
    {
        this.counterBuffersLength = counterBuffersLength;
        return this;
    }

    public StaticConfiguration counterBuffersFile(String counterBuffersFile)
    {
        this.counterBuffersFile = counterBuffersFile;
        return this;
    }

    public StaticConfiguration aeronChannel(final String aeronChannel)
    {
        this.aeronChannel = aeronChannel;
        return this;
    }

    public StaticConfiguration authenticationStrategy(final AuthenticationStrategy authenticationStrategy)
    {
        this.authenticationStrategy = authenticationStrategy;
        return this;
    }

    public StaticConfiguration newSessionHandler(final NewSessionHandler newSessionHandler)
    {
        this.newSessionHandler = newSessionHandler;
        return this;
    }

    public StaticConfiguration logFileDir(final String logFileDir)
    {
        this.logFileDir = logFileDir;
        return this;
    }

    public StaticConfiguration indexFileSize(final int indexFileSize)
    {
        this.indexFileSize = indexFileSize;
        return this;
    }

    public StaticConfiguration loggerCacheCapacity(int loggerCacheCapacity)
    {
        this.loggerCacheCapacity = loggerCacheCapacity;
        return this;
    }

    public StaticConfiguration beginString(final String beginString)
    {
        this.beginString = beginString.toCharArray();
        return this;
    }

    public StaticConfiguration setSendingTimeWindow(long sendingTimeWindow)
    {
        this.sendingTimeWindow = sendingTimeWindow;
        return this;
    }

    public StaticConfiguration connectionTimeout(final long connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public StaticConfiguration encoderBufferSize(final int encoderBufferSize)
    {
        this.encoderBufferSize = encoderBufferSize;
        return this;
    }

    public StaticConfiguration sessionCustomisationStrategy(final SessionCustomisationStrategy value)
    {
        this.sessionCustomisationStrategy = value;
        return this;
    }

    public StaticConfiguration logMessages(final boolean value)
    {
        this.logMessages = value;
        return this;
    }

    public int defaultHeartbeatInterval()
    {
        return defaultHeartbeatInterval;
    }

    public SessionIdStrategy sessionIdStrategy()
    {
        return sessionIdStrategy;
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

    public InetSocketAddress bindAddress()
    {
        return new InetSocketAddress(host, port);
    }

    public long connectionTimeout()
    {
        return connectionTimeout;
    }

    public int encoderBufferSize()
    {
        return encoderBufferSize;
    }

    public int counterBuffersLength()
    {
        return counterBuffersLength;
    }

    public String counterBuffersFile()
    {
        return counterBuffersFile;
    }

    public StaticConfiguration conclude()
    {
        return this;
    }

    public String aeronChannel()
    {
        return aeronChannel;
    }

    public AuthenticationStrategy authenticationStrategy()
    {
        return authenticationStrategy;
    }

    public NewSessionHandler newSessionHandler()
    {
        return newSessionHandler;
    }

    public String logFileDir()
    {
        return logFileDir;
    }

    public int indexFileSize()
    {
        return indexFileSize;
    }

    public int loggerCacheCapacity()
    {
        return loggerCacheCapacity;
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

    public boolean logMessages()
    {
        return logMessages;
    }
}
