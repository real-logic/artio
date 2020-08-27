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
package uk.co.real_logic.artio.engine;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.ErrorConsumer;
import uk.co.real_logic.artio.Clock;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.dictionary.SessionConstants;
import uk.co.real_logic.artio.engine.framer.DefaultTcpChannelSupplier;
import uk.co.real_logic.artio.engine.framer.TcpChannelSupplier;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.validation.AuthenticationProxy;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;
import uk.co.real_logic.artio.validation.SessionPersistenceStrategy;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.function.Function;

import static java.lang.Integer.getInteger;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.SECONDS;
import static uk.co.real_logic.artio.engine.logger.ReplayIndexDescriptor.INITIAL_RECORD_OFFSET;
import static uk.co.real_logic.artio.library.SessionConfiguration.*;
import static uk.co.real_logic.artio.validation.SessionPersistenceStrategy.alwaysTransient;

/**
 * Configuration that exists for the entire duration of a fix gateway. Some options are configurable via
 * commandline properties. Setters override commandline properties, not the other way around.
 * <p>
 * See setters or properties for documentation of what specific configuration options do.
 * <p>
 * NB: DO NOT REUSE this object over multiple {@code FixEngine.launch(EngineConfiguration)} calls.
 *
 * @see FixEngine
 */
public final class EngineConfiguration extends CommonConfiguration implements AutoCloseable
{
    // ------------------------------------------------
    //          Configuration Properties
    // ------------------------------------------------

    /**
     * Property name for the directory to log archive data into
     */
    public static final String LOG_FILE_DIR_PROP = "logging.dir";
    /**
     * Property name for size of logging index files
     */
    public static final String REPLAY_INDEX_FILE_SIZE_PROP = "logging.index.size";

    // Care needs to be taken when setting the fragment limits, and buffer sizes
    // The inbound bytes received and buffer sizes should always be set low enough
    // that fragments will be read off of the TCP connections slower
    // that they can be written onto them.

    /**
     * Property name for the max number of messages to read from libraries.
     */
    public static final String OUTBOUND_LIBRARY_FRAGMENT_LIMIT_PROP = "fix.core.outbound_fragment_limit";
    /**
     * Property name for the max number of messages to read from replayer.
     */
    public static final String REPLAY_FRAGMENT_LIMIT_PROP = "fix.core.replay_fragment_limit";
    /**
     * Property name for the max number of bytes to read from all TCP Connections.
     */
    public static final String INBOUND_BYTES_RECEIVED_LIMIT_PROP = "fix.core.inbound_bytes_limit";
    /**
     * Property name for the size in bytes of the receiver end point's framing buffer.
     */
    public static final String RECEIVER_BUFFER_SIZE_PROP = "fix.core.receiver_buffer_size";
    /**
     * Property name for the size in bytes of the TCP socket's receive buffer.
     */
    public static final String RECEIVER_SOCKET_BUFFER_SIZE_PROP = "fix.core.receiver_socket_buffer_size";
    /**
     * Property name for the size in bytes of the TCP socket's send buffer.
     */
    public static final String SENDER_SOCKET_BUFFER_SIZE_PROP = "fix.core.sender_socket_buffer_size";
    /**
     * Property name for the size in bytes of the sequence number cache file
     */
    public static final String SEQUENCE_NUMBER_INDEX_SIZE_PROP = "fix.core.sequence_number_cache_size";
    /**
     * Property name for the size in bytes of the session id file
     */
    public static final String SESSION_ID_BUFFER_SIZE_PROP = "fix.core.session_id_file_size";
    /**
     * Property name for the maximum number of bytes to allow in the quarantine buffer before disconnection
     */
    public static final String SENDER_MAX_BYTES_IN_BUFFER_PROP = "fix.core.sender_max_bytes_in_buffer";
    /**
     * Property name for the timeout before a connection that hasn't sent a logon is disconnected
     */
    public static final String NO_LOGON_DISCONNECT_TIMEOUT_PROP = "fix.core.no_logon_disconnect";

    // ------------------------------------------------
    //          Configuration Defaults
    // ------------------------------------------------

    public static final String DEFAULT_LOG_FILE_DIR = "logs";
    public static final int DEFAULT_REPLAY_INDEX_FILE_SIZE = 2 * 1024 * 1024 + INITIAL_RECORD_OFFSET;
    public static final int DEFAULT_LOGGER_CACHE_NUM_SETS = 8;
    public static final int DEFAULT_LOGGER_CACHE_SET_SIZE = 4;

    public static final int DEFAULT_OUTBOUND_LIBRARY_FRAGMENT_LIMIT = 20;
    public static final int DEFAULT_REPLAY_FRAGMENT_LIMIT = 5;
    public static final int DEFAULT_INBOUND_BYTES_RECEIVED_LIMIT = 8 * 1024;
    public static final int DEFAULT_RECEIVER_BUFFER_SIZE = 16 * 1024;
    public static final int DEFAULT_RECEIVER_SOCKET_BUFFER_SIZE = 1024 * 1024;
    public static final int DEFAULT_SENDER_SOCKET_BUFFER_SIZE = 1024 * 1024;
    public static final int DEFAULT_SEQUENCE_NUMBER_INDEX_SIZE = 8 * 1024 * 1024;
    public static final int DEFAULT_SESSION_ID_BUFFER_SIZE = 4 * 1024 * 1024;
    public static final int DEFAULT_SENDER_MAX_BYTES_IN_BUFFER = 4 * 1024 * 1024;
    public static final int DEFAULT_REPLAY_POSITION_BUFFER_SIZE = 4 * 1024;
    public static final int DEFAULT_NO_LOGON_DISCONNECT_TIMEOUT = (int)SECONDS.toMillis(5);
    public static final String DEFAULT_SESSION_ID_FILE = "session_id_buffer";
    public static final String DEFAULT_ILINK3_ID_FILE = "ilink3_id_buffer";
    public static final String DEFAULT_SEQUENCE_NUMBERS_SENT_FILE = "sequence_numbers_sent";
    public static final String DEFAULT_SEQUENCE_NUMBERS_RECEIVED_FILE = "sequence_numbers_received";
    public static final long DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS = 10_000;
    public static final ReplayHandler DEFAULT_REPLAY_HANDLER =
        (buffer, offset, length, libraryId, sessionId, sequenceIndex, messageType) ->
        {
        };
    public static final ILink3RetransmitHandler DEFAULT_ILINK3_RETRANSMIT_HANDLER =
        (templateId, buffer, offset, blockLength, version) ->
        {
        };

    /** Unmodifiable set of defaults, please make a copy if you wish to modify them. */
    public static final Set<String> DEFAULT_GAPFILL_ON_REPLAY_MESSAGE_TYPES;
    public static final long DEFAULT_INDEX_FILE_STATE_FLUSH_TIMEOUT_IN_MS = 10_000;
    public static final long DEFAULT_AUTHENTICATION_TIMEOUT_IN_MS = 60_000;
    public static final int DEFAULT_MAX_CONCURRENT_SESSION_REPLAYS = 5;

    static
    {
        final Set<String> defaultGapFillOnReplayMessageTypes = new HashSet<>();
        defaultGapFillOnReplayMessageTypes.add(SessionConstants.LOGON_MESSAGE_TYPE_STR);
        defaultGapFillOnReplayMessageTypes.add(SessionConstants.LOGOUT_MESSAGE_TYPE_STR);
        defaultGapFillOnReplayMessageTypes.add(SessionConstants.RESEND_REQUEST_MESSAGE_TYPE_STR);
        defaultGapFillOnReplayMessageTypes.add(SessionConstants.HEARTBEAT_MESSAGE_TYPE_STR);
        defaultGapFillOnReplayMessageTypes.add(SessionConstants.TEST_REQUEST_MESSAGE_TYPE_STR);
        defaultGapFillOnReplayMessageTypes.add(SessionConstants.SEQUENCE_RESET_TYPE_STR);
        DEFAULT_GAPFILL_ON_REPLAY_MESSAGE_TYPES = Collections.unmodifiableSet(defaultGapFillOnReplayMessageTypes);
    }

    public static final int DEFAULT_OUTBOUND_REPLAY_STREAM = 3;
    public static final int DEFAULT_ARCHIVE_REPLAY_STREAM = 4;
    public static final int DEFAULT_ARCHIVE_SCANNER_STREAM = 5;

    public static final int DEFAULT_INITIAL_SEQUENCE_INDEX = 0;

    private String host = null;
    private int port;
    private int replayIndexFileSize = getInteger(REPLAY_INDEX_FILE_SIZE_PROP, DEFAULT_REPLAY_INDEX_FILE_SIZE);
    private String logFileDir = getProperty(LOG_FILE_DIR_PROP, DEFAULT_LOG_FILE_DIR);
    private int loggerCacheNumSets = DEFAULT_LOGGER_CACHE_NUM_SETS;
    private int loggerCacheSetSize = DEFAULT_LOGGER_CACHE_SET_SIZE;
    private boolean logInboundMessages = true;
    private boolean logOutboundMessages = true;
    private boolean printStartupWarnings = true;
    private IdleStrategy framerIdleStrategy = backoffIdleStrategy();
    private IdleStrategy archiverIdleStrategy = backoffIdleStrategy();
    private AtomicBuffer sentSequenceNumberBuffer;
    private AtomicBuffer receivedSequenceNumberBuffer;
    private MappedFile sentSequenceNumberIndex;
    private MappedFile receivedSequenceNumberIndex;
    private MappedFile sessionIdBuffer;
    private MappedFile iLink3IdBuffer;
    private Set<String> gapfillOnReplayMessageTypes = new HashSet<>(DEFAULT_GAPFILL_ON_REPLAY_MESSAGE_TYPES);
    private IntHashSet gapfillOnRetransmitILinkTemplateIds = new IntHashSet();
    private final AeronArchive.Context archiveContext = new AeronArchive.Context();
    private AeronArchive.Context archiveContextClone;
    private Aeron.Context aeronContextClone;

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
    private final int sequenceNumberIndexSize =
        getInteger(SEQUENCE_NUMBER_INDEX_SIZE_PROP, DEFAULT_SEQUENCE_NUMBER_INDEX_SIZE);
    private final int sessionIdBufferSize =
        getInteger(SESSION_ID_BUFFER_SIZE_PROP, DEFAULT_SESSION_ID_BUFFER_SIZE);
    private int senderMaxBytesInBuffer =
        getInteger(SENDER_MAX_BYTES_IN_BUFFER_PROP, DEFAULT_SENDER_MAX_BYTES_IN_BUFFER);
    private int noLogonDisconnectTimeoutInMs =
        getInteger(NO_LOGON_DISCONNECT_TIMEOUT_PROP, DEFAULT_NO_LOGON_DISCONNECT_TIMEOUT);

    private String libraryAeronChannel = null;
    private Function<EngineConfiguration, TcpChannelSupplier> channelSupplierFactory = DefaultTcpChannelSupplier::new;
    private SessionPersistenceStrategy sessionPersistenceStrategy;
    private long slowConsumerTimeoutInMs = DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS;
    private EngineScheduler scheduler = new DefaultEngineScheduler();
    private ReplayHandler replayHandler = DEFAULT_REPLAY_HANDLER;
    private ILink3RetransmitHandler iLink3RetransmitHandler = DEFAULT_ILINK3_RETRANSMIT_HANDLER;
    private int outboundReplayStream = DEFAULT_OUTBOUND_REPLAY_STREAM;
    private int archiveReplayStream = DEFAULT_ARCHIVE_REPLAY_STREAM;
    private boolean acceptedSessionClosedResendInterval = DEFAULT_CLOSED_RESEND_INTERVAL;
    private int acceptedSessionResendRequestChunkSize = NO_RESEND_REQUEST_CHUNK_SIZE;
    private boolean acceptedSessionSendRedundantResendRequests = DEFAULT_SEND_REDUNDANT_RESEND_REQUESTS;
    private boolean acceptedEnableLastMsgSeqNumProcessed = DEFAULT_ENABLE_LAST_MSG_SEQ_NUM_PROCESSED;
    private InitialAcceptedSessionOwner initialAcceptedSessionOwner = InitialAcceptedSessionOwner.ENGINE;
    private AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.none();
    private long indexFileStateFlushTimeoutInMs = DEFAULT_INDEX_FILE_STATE_FLUSH_TIMEOUT_IN_MS;
    private FixDictionary acceptorfixDictionary;
    private boolean lookupDefaultAcceptorfixDictionary = true;
    private final Map<String, FixDictionary> acceptorFixDictionaryOverrides = new HashMap<>();
    private boolean deleteLogFileDirOnStart = false;
    private long authenticationTimeoutInMs = DEFAULT_AUTHENTICATION_TIMEOUT_IN_MS;
    private boolean bindAtStartup = false;
    private int initialSequenceIndex = DEFAULT_INITIAL_SEQUENCE_INDEX;
    private MessageTimingHandler messageTimingHandler = null;
    private int maxConcurrentSessionReplays = DEFAULT_MAX_CONCURRENT_SESSION_REPLAYS;
    private int replayPositionBufferSize = DEFAULT_REPLAY_POSITION_BUFFER_SIZE;

    /**
     * Sets the local address to bind to when the Gateway is used to accept connections.
     * <p>
     * Optional. If set defaults {@link #bindAtStartup(boolean)} to true. Care should be taken with the
     * initialisation order of these options.
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
        bindAtStartup(true);
        return this;
    }

    /**
     * Controls whether the engine should eagerly bind the network interface at startup when {@link #bindTo(String, int)} is used, or
     * whether it is delayed delayed until {@link FixEngine#bind()} is invoked. If used in conjunction with
     * {@link InitialAcceptedSessionOwner#SOLE_LIBRARY} then the binding operation will delayed until the library
     * is connected.
     *
     * @param bindAtStartup false to delay binding until {@link FixEngine#bind()} is invoked.
     * @return this
     */
    public EngineConfiguration bindAtStartup(final boolean bindAtStartup)
    {
        this.bindAtStartup = bindAtStartup;
        return this;
    }

    /**
     * Sets the receiver buffer size. This determines the maximum size of message that can be
     * received over the wire.
     *
     * @param receiverBufferSize the receiver buffer size.
     * @return this
     * @see CommonConfiguration#sessionBufferSize(int)
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
     * @see EngineConfiguration#LOG_FILE_DIR_PROP
     */
    public EngineConfiguration logFileDir(final String logFileDir)
    {
        this.logFileDir = logFileDir;
        return this;
    }

    /**
     * Sets the size of index files. This is the size in bytes for the replay index file that is used for each session.
     * If you want to size in terms of the last N Fix message fragments that you have received then
     * use the formula: INITIAL_RECORD_OFFSET + N * ReplayIndexDescriptor.RECORD_LENGTH.
     *
     * @param indexFileSize the size of index files.
     * @return this
     * @see EngineConfiguration#REPLAY_INDEX_FILE_SIZE_PROP
     * @see EngineConfiguration#DEFAULT_REPLAY_INDEX_FILE_SIZE
     */
    public EngineConfiguration replayIndexFileSize(final int indexFileSize)
    {
        this.replayIndexFileSize = indexFileSize;
        return this;
    }

    /**
     * Sets the set size of the logger's caches.
     * <p>
     * The logging and archival mechanism
     * has several caches of open memory mapped files which it stores streams of messages
     * into. This and {@link #loggerCacheNumSets} controls the size of those caches.
     * Should be increased if you see files being opened/closed in that area too frequently.
     * <p>
     * {@link org.agrona.collections.Int2ObjectCache} explains the difference between set size
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
     * @see #loggerCacheSetSize(int)
     *
     * @param loggerCacheNumSets the number of sets of in the logger's caches.
     * @return this
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
     * NB: if this configuration parameter is switched off then any replay requests will be gap filled.
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

    public EngineConfiguration printStartupWarnings(final boolean printStartupWarnings)
    {
        this.printStartupWarnings = printStartupWarnings;
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
     * @param archiverIdleStrategy the idle strategy for the Logger thread.
     * @return this
     */
    public EngineConfiguration archiverIdleStrategy(final IdleStrategy archiverIdleStrategy)
    {
        this.archiverIdleStrategy = archiverIdleStrategy;
        return this;
    }

    /**
     * Sets the fragment limit for the subscription to outbound messages from libraries.
     *
     * @param outboundLibraryFragmentLimit the fragment limit for the subscription to outbound messages from libraries.
     * @return this
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
     * @see EngineConfiguration#INBOUND_BYTES_RECEIVED_LIMIT_PROP
     */
    public EngineConfiguration inboundBytesReceivedLimit(final int inboundBytesReceivedLimit)
    {
        this.inboundBytesReceivedLimit = inboundBytesReceivedLimit;
        return this;
    }

    public EngineConfiguration senderMaxBytesInBuffer(final int senderMaxBytesInBuffer)
    {
        this.senderMaxBytesInBuffer = senderMaxBytesInBuffer;
        return this;
    }

    /**
     * Set the timeout in milliseconds for TCP connections which don't send a logon message.
     *
     * @param noLogonDisconnectTimeout the timeout in milliseconds for TCP connections which don't send a logon message
     * @return this
     */
    public EngineConfiguration noLogonDisconnectTimeoutInMs(final int noLogonDisconnectTimeout)
    {
        this.noLogonDisconnectTimeoutInMs = noLogonDisconnectTimeout;
        return this;
    }

    public EngineConfiguration channelSupplierFactory(final Function<EngineConfiguration, TcpChannelSupplier> value)
    {
        this.channelSupplierFactory = value;
        return this;
    }

    public EngineConfiguration sessionPersistenceStrategy(final SessionPersistenceStrategy sessionReplicationStrategy)
    {
        this.sessionPersistenceStrategy = sessionReplicationStrategy;
        return this;
    }

    public EngineConfiguration slowConsumerTimeoutInMs(final long slowConsumerTimeoutInMs)
    {
        this.slowConsumerTimeoutInMs = slowConsumerTimeoutInMs;
        return this;
    }

    public EngineConfiguration scheduler(final EngineScheduler scheduler)
    {
        this.scheduler = scheduler;
        return this;
    }

    /**
     * Sets the types of message that are gapfilled instead of replayed.
     *
     * When a resend request (2) arrives the gateway can choose to gap fill certain messages
     * instead of replaying them. A gap fill is implemented by sending a sequence reset message (4)
     * with it's gap fill flag set to true. This tells the FIX counter party that we won't be resending
     * those messages.
     *
     * By default Artio only gap fills administrative messages as the FIX spec demands. This method allows
     * you to customise it according to your needs.
     *
     * @see EngineConfiguration#DEFAULT_GAPFILL_ON_REPLAY_MESSAGE_TYPES
     * @param gapfillOnReplayMessageTypes the message types to gap fill
     * @return this
     */
    public EngineConfiguration gapfillOnReplayMessageTypes(final Set<String> gapfillOnReplayMessageTypes)
    {
        this.gapfillOnReplayMessageTypes = gapfillOnReplayMessageTypes;
        return this;
    }

    /**
     * Sets the types of template ids that are gapfilled instead of replayed in an Ilink3 connection.
     *
     * When a NotApplied message is handled by retransmitting message not all messages need to be retransmitted.
     * A gap fill here is implemented by sending a sequence message.
     *
     * @param gapfillOnRetransmitILinkTemplateIds the template ids to gap fill
     * @return this
     */
    public EngineConfiguration gapfillOnRetransmitILinkTemplateIds(
        final IntHashSet gapfillOnRetransmitILinkTemplateIds)
    {
        this.gapfillOnRetransmitILinkTemplateIds = gapfillOnRetransmitILinkTemplateIds;
        return this;
    }

    /**
     * Sets a handler that will be invoked when a message is replayed.
     *
     * @param replayHandler the replay handler
     * @return this
     */
    public EngineConfiguration replayHandler(final ReplayHandler replayHandler)
    {
        this.replayHandler = replayHandler;
        return this;
    }

    /**
     * Sets a handler that will be invoked when an iLink3 message is replayed.
     *
     * @param iLink3RetransmitHandler the replay handler
     * @return this
     */
    public EngineConfiguration iLink3RetransmitHandler(final ILink3RetransmitHandler iLink3RetransmitHandler)
    {
        this.iLink3RetransmitHandler = iLink3RetransmitHandler;
        return this;
    }

    public EngineConfiguration outboundReplayStream(final int outboundReplayStream)
    {
        this.outboundReplayStream = outboundReplayStream;
        return this;
    }

    public EngineConfiguration archiveReplayStream(final int archiveReplayStream)
    {
        this.archiveReplayStream = archiveReplayStream;
        return this;
    }

    /**
     * Sets the {@link SessionConfiguration#closedResendInterval()} property for accepted Sessions.
     *
     * @param acceptedSessionClosedResendInterval the {@link SessionConfiguration#closedResendInterval()} property for
     *                                           accepted Sessions.
     * @return this
     */
    public EngineConfiguration acceptedSessionClosedResendInterval(final boolean acceptedSessionClosedResendInterval)
    {
        this.acceptedSessionClosedResendInterval = acceptedSessionClosedResendInterval;
        return this;
    }

    /**
     * Sets the {@link SessionConfiguration#resendRequestChunkSize()} property for accepted Sessions.
     *
     * @param acceptedSessionResendRequestChunkSize the {@link SessionConfiguration#resendRequestChunkSize()} property
     *                                             for accepted Sessions.
     * @return this
     */
    public EngineConfiguration acceptedSessionResendRequestChunkSize(final int acceptedSessionResendRequestChunkSize)
    {
        this.acceptedSessionResendRequestChunkSize = acceptedSessionResendRequestChunkSize;
        return this;
    }

    /**
     * Sets the {@link SessionConfiguration#sendRedundantResendRequests()} property for accepted Sessions.
     *
     * @param acceptedSessionSendRedundantResendRequests the {@link SessionConfiguration#sendRedundantResendRequests()}
     *                                                   property for accepted Sessions.
     * @return this
     */
    public EngineConfiguration acceptedSessionSendRedundantResendRequests(
        final boolean acceptedSessionSendRedundantResendRequests)
    {
        this.acceptedSessionSendRedundantResendRequests = acceptedSessionSendRedundantResendRequests;
        return this;
    }

    /**
     * Sets the {@link SessionConfiguration#enableLastMsgSeqNumProcessed()} property for accepted Sessions.
     *
     * @param acceptedEnableLastMsgSeqNumProcessed the {@link SessionConfiguration#enableLastMsgSeqNumProcessed()}
     *                                             property for accepted Sessions.
     * @return this
     */
    public EngineConfiguration acceptedEnableLastMsgSeqNumProcessed(
        final boolean acceptedEnableLastMsgSeqNumProcessed)
    {
        this.acceptedEnableLastMsgSeqNumProcessed = acceptedEnableLastMsgSeqNumProcessed;
        return this;
    }

    /**
     * Set whether accepted sessions are initially owned by the Engine or a Library - the default is
     * the Engine. When a FIX initiator initially connects to an Artio acceptor then by default this session is owned
     * by the Engine and Libraries can request ownership of the Session from the Engine. If <code>SOLE_LIBRARY</code>
     * mode is chosen then only a single library instance must connect.
     *
     * In sole
     * library mode the server side TCP port will automatically be bound when the sole library connects, and unbound
     * when it disconnects.
     *
     * @param initialAcceptedSessionOwner whether accepted sessions are initially owned by the Engine or a Library
     * @return this
     */
    public EngineConfiguration initialAcceptedSessionOwner(
        final InitialAcceptedSessionOwner initialAcceptedSessionOwner)
    {
        this.initialAcceptedSessionOwner = initialAcceptedSessionOwner;
        return this;
    }

    /**
     * Sets the aeron channel that libraries will use to communicate with this FixEngine instance.
     *
     * @param libraryAeronChannel the aeron channel that libraries will use to communicate with this FixEngine instance.
     * @return this
     */
    public EngineConfiguration libraryAeronChannel(final String libraryAeronChannel)
    {
        this.libraryAeronChannel = libraryAeronChannel;
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
    public EngineConfiguration authenticationStrategy(final AuthenticationStrategy authenticationStrategy)
    {
        this.authenticationStrategy = authenticationStrategy;
        return this;
    }

    public EngineConfiguration indexFileStateFlushTimeoutInMs(final long indexFileStateFlushTimeoutInMs)
    {
        this.indexFileStateFlushTimeoutInMs = indexFileStateFlushTimeoutInMs;
        return this;
    }

    /**
     * Specify a single acceptor FIX Dictionary. If an override for a given FIX version is specified using
     * {@link #overrideAcceptorFixDictionary(Class)} then this FIX Dictionary will be used as a catch-all for any
     * connections that don't match the versions in the overrides.
     *
     * Note, that if this dictionary isn't specified then the default path that is
     * generated by the CodecGenerationTool will be used.
     *
     * @param acceptorfixDictionary the Fix Dictionary used to parse messages when accepting an inbound connection.
     * @return this
     * @see EngineConfiguration#overrideAcceptorFixDictionary(Class)
     * @see AuthenticationProxy#accept(Class)
     */
    public EngineConfiguration acceptorfixDictionary(final Class<? extends FixDictionary> acceptorfixDictionary)
    {
        this.acceptorfixDictionary = FixDictionary.of(acceptorfixDictionary);
        return this;
    }

    /**
     * Can be used to disable the automated lookup of an acceptorFixDictionary. This is useful when you're not
     * using regular FIX as your protocol but something else for example - Artio's iLink3 support.
     *
     * @param lookupDefaultAcceptorfixDictionary true if you want to lookup a default acceptor fix dictionary (the
     *                                           default), false otherwise.
     * @return this
     */
    public EngineConfiguration lookupDefaultAcceptorfixDictionary(final boolean lookupDefaultAcceptorfixDictionary)
    {
        this.lookupDefaultAcceptorfixDictionary = lookupDefaultAcceptorfixDictionary;
        return this;
    }

    /**
     * Override the acceptor FIX Dictionary for a given beginString. The beginString to use is extracted from the
     * Provided FIX Dictionary. If you wish to use multiple FIX dictionaries based upon the logon message and they
     * both have the same beginString field then you should should implement a custom {@link AuthenticationStrategy}
     * and use the {@link AuthenticationProxy#accept(Class)} method in order to specify the dictionary.
     *
     * @param fixDictionaryClass the FIX Dictionary to use
     * @return this
     * @see EngineConfiguration#acceptorfixDictionary(Class)
     * @see AuthenticationProxy#accept(Class)
     */
    public EngineConfiguration overrideAcceptorFixDictionary(final Class<? extends FixDictionary> fixDictionaryClass)
    {
        final FixDictionary dictionary = FixDictionary.of(fixDictionaryClass);
        this.acceptorFixDictionaryOverrides.put(dictionary.beginString(), dictionary);
        return this;
    }

    public EngineConfiguration deleteLogFileDirOnStart(final boolean deleteLogFileDirOnStart)
    {
        this.deleteLogFileDirOnStart = deleteLogFileDirOnStart;
        return this;
    }

    public EngineConfiguration authenticationTimeoutInMs(final long authenticationTimeoutInMs)
    {
        this.authenticationTimeoutInMs = authenticationTimeoutInMs;
        return this;
    }

    public EngineConfiguration messageTimingHandler(final MessageTimingHandler messageTimingHandler)
    {
        this.messageTimingHandler = messageTimingHandler;
        return this;
    }

    /**
     * Sets the maximum number of resend requests per session that Artio will process concurrently. Once the maximum is
     * hit further FIX resend requests will be ignored and an Exception will be logged noting the event. Note
     * this is a per-session parameter - ie the number of ResendRequests that will be queued for processing.
     *
     * @param maxConcurrentSessionReplays the maximum number of resend requests per session that Artio will enqueue
     *                                    before rejecting any more.
     * @return this
     */
    public EngineConfiguration maxConcurrentSessionReplays(final int maxConcurrentSessionReplays)
    {
        this.maxConcurrentSessionReplays = maxConcurrentSessionReplays;
        return this;
    }

    /**
     * Sets the initial sequenceIndex for the new session.
     * Doesnt affects existing session.
     *
     * @param initialSequenceIndex initial sequence index
     * @return this
     */
    public EngineConfiguration initialSequenceIndex(final int initialSequenceIndex)
    {
        this.initialSequenceIndex = initialSequenceIndex;
        return this;
    }

    public EngineConfiguration replayPositionBufferSize(final int replayPositionBufferSize)
    {
        this.replayPositionBufferSize = replayPositionBufferSize;
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

    public boolean bindAtStartup()
    {
        return this.bindAtStartup;
    }

    public String logFileDir()
    {
        return logFileDir;
    }

    public int replayIndexFileSize()
    {
        return replayIndexFileSize;
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

    public boolean printStartupWarnings()
    {
        return printStartupWarnings;
    }

    boolean logAnyMessages()
    {
        return logInboundMessages || logOutboundMessages;
    }

    public boolean logAllMessages()
    {
        return logInboundMessages && logOutboundMessages;
    }

    public IdleStrategy framerIdleStrategy()
    {
        return framerIdleStrategy;
    }

    public IdleStrategy archiverIdleStrategy()
    {
        return archiverIdleStrategy;
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

    public MappedFile iLink3IdBuffer()
    {
        if (iLink3IdBuffer == null)
        {
            iLink3IdBuffer = mapFile(DEFAULT_ILINK3_ID_FILE, sessionIdBufferSize);
        }
        return iLink3IdBuffer;
    }

    public Set<String> gapfillOnReplayMessageTypes()
    {
        return gapfillOnReplayMessageTypes;
    }

    public IntHashSet gapfillOnRetransmitILinkTemplateIds()
    {
        return gapfillOnRetransmitILinkTemplateIds;
    }

    public int senderMaxBytesInBuffer()
    {
        return senderMaxBytesInBuffer;
    }

    public int noLogonDisconnectTimeoutInMs()
    {
        return noLogonDisconnectTimeoutInMs;
    }

    public SessionPersistenceStrategy sessionPersistenceStrategy()
    {
        return sessionPersistenceStrategy;
    }

    public EngineScheduler scheduler()
    {
        return scheduler;
    }

    public long slowConsumerTimeoutInMs()
    {
        return slowConsumerTimeoutInMs;
    }

    public ReplayHandler replayHandler()
    {
        return replayHandler;
    }

    public ILink3RetransmitHandler iLink3RetransmitHandler()
    {
        return iLink3RetransmitHandler;
    }

    public InitialAcceptedSessionOwner initialAcceptedSessionOwner()
    {
        return initialAcceptedSessionOwner;
    }

    public AuthenticationStrategy authenticationStrategy()
    {
        return authenticationStrategy;
    }

    public long indexFileStateFlushTimeoutInMs()
    {
        return indexFileStateFlushTimeoutInMs;
    }

    public FixDictionary acceptorfixDictionary()
    {
        return acceptorfixDictionary;
    }

    public Map<String, FixDictionary> acceptorFixDictionaryOverrides()
    {
        return acceptorFixDictionaryOverrides;
    }

    public boolean deleteLogFileDirOnStart()
    {
        return deleteLogFileDirOnStart;
    }

    public long authenticationTimeoutInMs()
    {
        return authenticationTimeoutInMs;
    }

    public int initialSequenceIndex()
    {
        return initialSequenceIndex;
    }

    public int maxConcurrentSessionReplays()
    {
        return maxConcurrentSessionReplays;
    }

    public int replayPositionBufferSize()
    {
        return replayPositionBufferSize;
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
    public EngineConfiguration monitoringFile(final String monitoringFile)
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

    /**
     * {@inheritDoc}
     */
    public EngineConfiguration agentNamePrefix(final String agentNamePrefix)
    {
        super.agentNamePrefix(agentNamePrefix);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public EngineConfiguration printAeronStreamIdentifiers(final boolean printAeronStreamIdentifiers)
    {
        super.printAeronStreamIdentifiers(printAeronStreamIdentifiers);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public EngineConfiguration clock(final Clock clock)
    {
        super.clock(clock);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public EngineConfiguration inboundLibraryStream(final int inboundLibraryStream)
    {
        super.inboundLibraryStream(inboundLibraryStream);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public EngineConfiguration outboundLibraryStream(final int outboundLibraryStream)
    {
        super.outboundLibraryStream(outboundLibraryStream);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public EngineConfiguration printErrorMessages(final boolean printErrorMessages)
    {
        super.printErrorMessages(printErrorMessages);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    public EngineConfiguration customErrorConsumer(final ErrorConsumer customErrorConsumer)
    {
        super.customErrorConsumer(customErrorConsumer);
        return this;
    }

    public AeronArchive.Context aeronArchiveContext()
    {
        return archiveContext;
    }

    public Aeron.Context aeronContextClone()
    {
        return aeronContextClone;
    }

    public AeronArchive.Context archiveContextClone()
    {
        return archiveContextClone;
    }

    public int outboundReplayStream()
    {
        return outboundReplayStream;
    }

    public int archiveReplayStream()
    {
        return archiveReplayStream;
    }

    public boolean acceptedSessionClosedResendInterval()
    {
        return acceptedSessionClosedResendInterval;
    }

    public int acceptedSessionResendRequestChunkSize()
    {
        return acceptedSessionResendRequestChunkSize;
    }

    public boolean acceptedSessionSendRedundantResendRequests()
    {
        return acceptedSessionSendRedundantResendRequests;
    }

    public boolean acceptedEnableLastMsgSeqNumProcessed()
    {
        return acceptedEnableLastMsgSeqNumProcessed;
    }

    public MessageTimingHandler messageTimingHandler()
    {
        return messageTimingHandler;
    }

    public EngineConfiguration conclude()
    {
        super.conclude("engine");

        if (libraryAeronChannel() == null)
        {
            throw new IllegalArgumentException("Missing required configuration: library aeron channel");
        }

        if (bindAtStartup() && !hasBindAddress())
        {
            throw new IllegalArgumentException("If you're setting EngineConfiguration.bindAtStartup() then you must " +
                "also specify an address to bind to using EngineConfiguration.bindTo(host,port)");
        }

        if (receiverBufferSize() < sessionBufferSize())
        {
            throw new IllegalArgumentException(String.format(
                "You cannot set the receiverBufferSize(%d) < sessionBufferSize(%d)." +
                    "this would allow you to encode messages that are larger than you can read.",
                receiverBufferSize(),
                sessionBufferSize()));
        }

        if (deleteLogFileDirOnStart())
        {
            final File logFileDir = new File(logFileDir());
            if (logFileDir.exists())
            {
                IoUtil.delete(logFileDir, false);
            }
        }

        if (sentSequenceNumberIndex() == null)
        {
            sentSequenceNumberIndex = mapFile(DEFAULT_SEQUENCE_NUMBERS_SENT_FILE, sequenceNumberIndexSize);
        }

        if (sentSequenceNumberBuffer() == null)
        {
            sentSequenceNumberBuffer = new UnsafeBuffer(new byte[sequenceNumberIndexSize]);
        }

        if (receivedSequenceNumberIndex() == null)
        {
            receivedSequenceNumberIndex = mapFile(DEFAULT_SEQUENCE_NUMBERS_RECEIVED_FILE, sequenceNumberIndexSize);
        }

        if (receivedSequenceNumberBuffer() == null)
        {
            receivedSequenceNumberBuffer = new UnsafeBuffer(new byte[sequenceNumberIndexSize]);
        }

        if (sessionIdBuffer() == null)
        {
            sessionIdBuffer = mapFile(DEFAULT_SESSION_ID_FILE, sessionIdBufferSize);
        }

        if (sessionPersistenceStrategy() == null)
        {
            sessionPersistenceStrategy(alwaysTransient());
        }

        if (lookupDefaultAcceptorfixDictionary && acceptorfixDictionary() == null)
        {
            acceptorfixDictionary(FixDictionary.findDefault());
        }

        aeronContextClone = aeronContext().clone();
        archiveContextClone = archiveContext.clone();

        return this;
    }

    private MappedFile mapFile(final String file, final int size)
    {
        return MappedFile.map(logFileDir() + File.separator + file, size);
    }

    public String libraryAeronChannel()
    {
        return libraryAeronChannel;
    }

    public TcpChannelSupplier channelSupplier()
    {
        return channelSupplierFactory.apply(this);
    }

    public boolean isRelevantStreamId(final int streamId)
    {
        return (streamId == outboundLibraryStream() && logOutboundMessages()) ||
            (streamId == inboundLibraryStream() && logInboundMessages());
    }

    public void close()
    {
        CloseHelper.close(sentSequenceNumberIndex);
        CloseHelper.close(receivedSequenceNumberIndex);
        CloseHelper.close(sessionIdBuffer);
        CloseHelper.close(iLink3IdBuffer);
    }
}
