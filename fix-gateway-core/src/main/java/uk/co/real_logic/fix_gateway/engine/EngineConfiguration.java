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

import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.CommonConfiguration;
import uk.co.real_logic.fix_gateway.otf.OtfMessageAcceptor;

import java.net.InetSocketAddress;
import java.util.stream.IntStream;

import static java.lang.Integer.getInteger;
import static java.lang.System.getProperty;

/**
 * Configuration that exists for the entire duration of a fix gateway
 */
public final class EngineConfiguration extends CommonConfiguration
{
    // ------------------------------------------------
    //          Configuration Properties
    // ------------------------------------------------

    /** Property name for the directory to log archive data into */
    public static final String LOG_FILE_DIR_PROP = "logging.dir";
    /** Property name for size of logging index files */
    public static final String INDEX_FILE_SIZE_PROP = "logging.index.size";

    // ------------------------------------------------
    //          Configuration Defaults
    // ------------------------------------------------

    public static final int DEFAULT_RECEIVER_BUFFER_SIZE = 1024 * 1024;
    public static final int DEFAULT_INDEX_FILE_SIZE = 2 * 1024 * 1024;
    public static final String DEFAULT_LOG_FILE_DIR = "logs";
    public static final int DEFAULT_LOGGER_CACHE_CAPACITY = 10;
    private final Int2ObjectHashMap<OtfMessageAcceptor> otfAcceptors = new Int2ObjectHashMap<>();

    private int receiverBufferSize = DEFAULT_RECEIVER_BUFFER_SIZE;
    private int receiverSocketBufferSize = 1024 * 1024;
    private int senderSocketBufferSize = 1024 * 1024;
    private String host;
    private int port;
    private int indexFileSize = getInteger(INDEX_FILE_SIZE_PROP, DEFAULT_INDEX_FILE_SIZE);
    private String logFileDir = getProperty(LOG_FILE_DIR_PROP, DEFAULT_LOG_FILE_DIR);
    private int loggerCacheCapacity = DEFAULT_LOGGER_CACHE_CAPACITY;
    private boolean logInboundMessages = true;
    private boolean logOutboundMessages = true;
    private boolean printErrorMessages = true;
    private IdleStrategy framerIdleStrategy = backoffIdleStrategy();
    private IdleStrategy loggerIdleStrategy = backoffIdleStrategy();
    private IdleStrategy errorPrinterIdleStrategy = new BackoffIdleStrategy(1, 1, 1000, 1_000_000);

    public EngineConfiguration registerAcceptor(
        final OtfMessageAcceptor messageAcceptor, int firstTag, final int... tags)
    {
        otfAcceptors.put(firstTag, messageAcceptor);
        IntStream.of(tags).forEach(tag -> otfAcceptors.put(tag, messageAcceptor));
        return this;
    }

    public EngineConfiguration bind(final String host, final int port)
    {
        this.host = host;
        this.port = port;
        return this;
    }

    public EngineConfiguration receiverBufferSize(final int value)
    {
        receiverBufferSize = value;
        return this;
    }

    public EngineConfiguration receiverSocketBufferSize(final int value)
    {
        receiverSocketBufferSize = value;
        return this;
    }

    public EngineConfiguration senderSocketBufferSize(final int value)
    {
        senderSocketBufferSize = value;
        return this;
    }

    public EngineConfiguration logFileDir(final String logFileDir)
    {
        this.logFileDir = logFileDir;
        return this;
    }

    public EngineConfiguration indexFileSize(final int indexFileSize)
    {
        this.indexFileSize = indexFileSize;
        return this;
    }

    public EngineConfiguration loggerCacheCapacity(int loggerCacheCapacity)
    {
        this.loggerCacheCapacity = loggerCacheCapacity;
        return this;
    }


    public EngineConfiguration connectionTimeout(final long connectionTimeout)
    {
        return this;
    }

    public EngineConfiguration logInboundMessages(final boolean value)
    {
        this.logInboundMessages = value;
        return this;
    }

    public EngineConfiguration logOutboundMessages(final boolean value)
    {
        this.logOutboundMessages = value;
        return this;
    }

    public EngineConfiguration printErrorMessages(final boolean printErrorMessages)
    {
        this.printErrorMessages = printErrorMessages;
        return this;
    }

    public EngineConfiguration framerIdleStrategy(final IdleStrategy framerIdleStrategy)
    {
        this.framerIdleStrategy = framerIdleStrategy;
        return this;
    }

    public EngineConfiguration loggerIdleStrategy(final IdleStrategy loggerIdleStrategy)
    {
        this.loggerIdleStrategy = loggerIdleStrategy;
        return this;
    }

    public EngineConfiguration errorPrinterIdleStrategy(final IdleStrategy errorPrinterIdleStrategy)
    {
        this.errorPrinterIdleStrategy = errorPrinterIdleStrategy;
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

    public int loggerCacheCapacity()
    {
        return loggerCacheCapacity;
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

    public EngineConfiguration aeronChannel(final String aeronChannel)
    {
        super.aeronChannel(aeronChannel);
        return this;
    }

    public EngineConfiguration counterBuffersLength(final Integer counterBuffersLength)
    {
        super.counterBuffersLength(counterBuffersLength);
        return this;
    }

    public EngineConfiguration monitoringFile(String counterBuffersFile)
    {
        super.monitoringFile(counterBuffersFile);
        return this;
    }

    public EngineConfiguration replyTimeoutInMs(final long replyTimeoutInMs)
    {
        super.replyTimeoutInMs(replyTimeoutInMs);
        return this;
    }

}
