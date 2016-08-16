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
package uk.co.real_logic.fix_gateway;

import io.aeron.Aeron;
import org.agrona.IoUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.session.SessionCustomisationStrategy;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.validation.MessageValidationStrategy;

import java.io.File;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Integer.getInteger;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Common configuration for both the Fix Engine and Library. Some options are configurable via
 * commandline properties. Setters override commandline properties, not the other way around.
 * <p>
 * See setters or properties for documentation of what specific configuration options do.
 *
 * @see uk.co.real_logic.fix_gateway.engine.EngineConfiguration
 * @see uk.co.real_logic.fix_gateway.library.LibraryConfiguration
 */
public class CommonConfiguration
{

    // ------------------------------------------------
    //          Configuration Properties
    // ------------------------------------------------

    /** Property name for length of the memory mapped buffers for the counters file */
    public static final String MONITORING_BUFFERS_LENGTH_PROPERTY = "fix.monitoring.length";
    /** Property name for directory of the conductor buffers */
    public static final String MONITORING_FILE_PROPERTY = "fix.monitoring.file";
    /** Property name for the flag to enable or disable debug logging */
    public static final String DEBUG_PRINT_MESSAGES_PROPERTY = "fix.core.debug";
    /** Property name for the flag to enable or disable flushing of writes */
    public static final String FORCE_WRITES_MESSAGES_PROPERTY = "fix.core.flush";
    /**
     * Property name for the flag to set the maximum number of attempts to claim a message
     * slot on the inbound stream.
     */
    public static final String INBOUND_MAX_CLAIM_ATTEMPTS_PROPERTY = "fix.core.inbound_max_claims";
    /**
     * Property name for the flag to set the maximum number of attempts to claim a message
     * slot on the outbound stream.
     */
    public static final String OUTBOUND_MAX_CLAIM_ATTEMPTS_PROPERTY = "fix.core.outbound_max_claims";
    /** Property name for the flag to enable or disable message timing */
    public static final String TIME_MESSAGES_PROPERTY = "fix.core.timing";
    /** Property name for the file to log debug messages to, default is standard output */
    public static final String DEBUG_FILE_PROPERTY = "fix.core.debug.file";
    /** Property name for the period at which histogram intervals are polled and logged */
    public static final String HISTOGRAM_POLL_PERIOD_IN_MS_PROPERTY = "fix.benchmark.histogram_poll_period";
    /** Property name for the file to which histogram intervals are logged */
    public static final String HISTOGRAM_LOGGING_FILE_PROPERTY = "fix.benchmark.histogram_file";

    public static final int DEFAULT_MONITORING_BUFFER_LENGTH = 8 * 1024 * 1024;
    public static final String DEFAULT_MONITORING_FILE =
        optimalTmpDirName() + File.separator + "fix-%s" + File.separator + "monitoring";

    public static final String DEFAULT_HISTOGRAM_LOGGING_FILE =
        optimalTmpDirName() + File.separator + "fix-%s" + File.separator + "histograms";

    // ------------------------------------------------
    //          Static Configuration
    // ------------------------------------------------

    /** These are static final fields in order to give the optimiser more scope */
    public static final boolean DEBUG_PRINT_MESSAGES;
    public static final Set<LogTag> DEBUG_TAGS;
    static
    {
        final String property = getProperty(DEBUG_PRINT_MESSAGES_PROPERTY);
        boolean debugPrintMessages = false;
        Set<LogTag> debugTags = Collections.emptySet();
        if (property != null)
        {
            if ("all".equals(property) || "true".equals(property))
            {
                debugPrintMessages = true;
                debugTags = EnumSet.allOf(LogTag.class);
            }
            else
            {
                try
                {
                    debugTags =
                        Stream.of(property.split(","))
                            .map(LogTag::valueOf)
                            .collect(Collectors.toCollection(() -> EnumSet.noneOf(LogTag.class)));

                    debugPrintMessages = !debugTags.isEmpty();
                }
                catch (final IllegalArgumentException e)
                {
                    // parse error in valueOf();
                }
            }
        }

        DEBUG_PRINT_MESSAGES = debugPrintMessages;
        DEBUG_TAGS = debugTags;
    }

    public static final String DEBUG_FILE = System.getProperty(DEBUG_FILE_PROPERTY);
    public static final boolean TIME_MESSAGES = Boolean.getBoolean(TIME_MESSAGES_PROPERTY);
    public static final boolean FORCE_WRITES = Boolean.getBoolean(FORCE_WRITES_MESSAGES_PROPERTY);

    public static final int BACKOFF_SPINS = Integer.getInteger("fix.core.spins", 1_000);
    public static final int BACKOFF_YIELDS = Integer.getInteger("fix.core.yields", 10_000);

    // ------------------------------------------------
    //          Configuration Defaults
    // ------------------------------------------------

    public static final int DEFAULT_INBOUND_MAX_CLAIM_ATTEMPTS = BACKOFF_SPINS + BACKOFF_YIELDS + 1000;
    public static final int DEFAULT_OUTBOUND_MAX_CLAIM_ATTEMPTS = DEFAULT_INBOUND_MAX_CLAIM_ATTEMPTS;

    public static final int DEFAULT_SESSION_BUFFER_SIZE = 8 * 1024;
    public static final long DEFAULT_SENDING_TIME_WINDOW = MINUTES.toMillis(2);
    public static final int DEFAULT_HEARTBEAT_INTERVAL_IN_S = 10;

    private static final long DEFAULT_REPLY_TIMEOUT_IN_MS = 10_000L;
    private static final int DEFAULT_ERROR_SLOT_SIZE = 1024;
    private static final boolean ACCEPTOR_SEQUENCE_NUMBERS_RESET_UPON_RECONNECT_DEFAULT = true;
    public static final long DEFAULT_HISTOGRAM_POLL_PERIOD_IN_MS = MINUTES.toMillis(1);

    private boolean printErrorMessages = true;
    private IdleStrategy errorPrinterIdleStrategy = new BackoffIdleStrategy(1, 1, 1000, 1_000_000);
    private long sendingTimeWindowInMs = DEFAULT_SENDING_TIME_WINDOW;
    private SessionIdStrategy sessionIdStrategy = SessionIdStrategy.senderAndTarget();
    private AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.none();
    private MessageValidationStrategy messageValidationStrategy = MessageValidationStrategy.none();
    private SessionCustomisationStrategy sessionCustomisationStrategy = SessionCustomisationStrategy.none();
    private int monitoringBuffersLength = getInteger(MONITORING_BUFFERS_LENGTH_PROPERTY, DEFAULT_MONITORING_BUFFER_LENGTH);
    private String monitoringFile = null;
    private long replyTimeoutInMs = DEFAULT_REPLY_TIMEOUT_IN_MS;
    private Aeron.Context aeronContext = new Aeron.Context();
    private int errorSlotSize = DEFAULT_ERROR_SLOT_SIZE;
    private int sessionBufferSize = DEFAULT_SESSION_BUFFER_SIZE;

    private int inboundMaxClaimAttempts =
        getInteger(INBOUND_MAX_CLAIM_ATTEMPTS_PROPERTY, DEFAULT_INBOUND_MAX_CLAIM_ATTEMPTS);
    private int outboundMaxClaimAttempts =
        getInteger(OUTBOUND_MAX_CLAIM_ATTEMPTS_PROPERTY, DEFAULT_OUTBOUND_MAX_CLAIM_ATTEMPTS);
    private int defaultHeartbeatIntervalInS = DEFAULT_HEARTBEAT_INTERVAL_IN_S;
    private boolean acceptorSequenceNumbersResetUponReconnect = ACCEPTOR_SEQUENCE_NUMBERS_RESET_UPON_RECONNECT_DEFAULT;
    private long histogramPollPeriodInMs =
        Long.getLong(HISTOGRAM_POLL_PERIOD_IN_MS_PROPERTY, DEFAULT_HISTOGRAM_POLL_PERIOD_IN_MS);
    private String histogramLoggingFile = null;

    /**
     * Sets the sending time window. The sending time window is the period of acceptance
     * delta between the current time on the Fix Library thread and the sending time
     * received in messages. Sessions are disconnected if the sending time diverges by
     * more than this window and if validation is enabled.
     *
     * @param sendingTimeWindowInMs the current sending time in milliseconds
     * @return this
     */
    public CommonConfiguration sendingTimeWindowInMs(long sendingTimeWindowInMs)
    {
        this.sendingTimeWindowInMs = sendingTimeWindowInMs;
        return this;
    }

    public long sendingTimeWindowInMs()
    {
        return sendingTimeWindowInMs;
    }

    /**
     * The default interval for heartbeats if not exchanged upon logon. Specified in seconds.
     *
     * @return this
     */
    public CommonConfiguration defaultHeartbeatIntervalInS(final int value)
    {
        defaultHeartbeatIntervalInS = value;
        return this;
    }

    public int defaultHeartbeatIntervalInS()
    {
        return defaultHeartbeatIntervalInS;
    }

    /**
     * Configure whether you want the session to reset its sequence number when it reconnects.
     * The session is determined to be the same if the session id strategy allocates it the same
     * id.
     *
     * @param value true if you want them to reset
     * @return this configuration object.
     *
     * @see uk.co.real_logic.fix_gateway.library.SessionConfiguration#sequenceNumbersPersistent()
     * @see this#sessionIdStrategy(SessionIdStrategy)
     */
    public CommonConfiguration acceptorSequenceNumbersResetUponReconnect(final boolean value)
    {
        this.acceptorSequenceNumbersResetUponReconnect = value;
        return this;
    }

    public boolean acceptorSequenceNumbersResetUponReconnect()
    {
        return acceptorSequenceNumbersResetUponReconnect;
    }

    /**
     * Sets the session id strategy.
     *
     * @param sessionIdStrategy the session id strategy.
     * @return this
     *
     * @see SessionIdStrategy
     */
    public CommonConfiguration sessionIdStrategy(final SessionIdStrategy sessionIdStrategy)
    {
        this.sessionIdStrategy = sessionIdStrategy;
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
    public CommonConfiguration authenticationStrategy(final AuthenticationStrategy authenticationStrategy)
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
    public CommonConfiguration sessionCustomisationStrategy(
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
    public CommonConfiguration messageValidationStrategy(final MessageValidationStrategy messageValidationStrategy)
    {
        this.messageValidationStrategy = messageValidationStrategy;
        return this;
    }

    /**
     * Sets the length of the buffer used for monitoring counters.
     *
     * @param monitoringBuffersLength the length of the buffer used for monitoring counters.
     * @return this
     *
     * @see CommonConfiguration#MONITORING_BUFFERS_LENGTH_PROPERTY
     */
    public CommonConfiguration monitoringBuffersLength(final Integer monitoringBuffersLength)
    {
        this.monitoringBuffersLength = monitoringBuffersLength;
        return this;
    }

    /**
     * Sets the location for the monitoring file.
     *
     * @param monitoringFile the location for the monitoring file.
     * @return this
     *
     * @see CommonConfiguration#MONITORING_FILE_PROPERTY
     */
    public CommonConfiguration monitoringFile(String monitoringFile)
    {
        this.monitoringFile = monitoringFile;
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
    public CommonConfiguration printErrorMessages(final boolean printErrorMessages)
    {
        this.printErrorMessages = printErrorMessages;
        return this;
    }

    /**
     * Sets the idle strategy for the Error Printer thread.
     *
     * @param errorPrinterIdleStrategy the idle strategy for the Error Printer thread.
     * @return this
     */
    public CommonConfiguration errorPrinterIdleStrategy(final IdleStrategy errorPrinterIdleStrategy)
    {
        this.errorPrinterIdleStrategy = errorPrinterIdleStrategy;
        return this;
    }

    /**
     * Sets the reply timeout in milliseconds.
     * <p>
     * This is the timeout for control protocol messages between the FIX Gateway and FIX Library instances.
     *
     * @param replyTimeoutInMs the reply timeout in milliseconds.
     * @return this
     */
    public CommonConfiguration replyTimeoutInMs(final long replyTimeoutInMs)
    {
        this.replyTimeoutInMs = replyTimeoutInMs;
        return this;
    }

    /**
     * Sets the error slot size. The error slot size is the number of different types of errors that are
     * simultaneously held in the error buffer.
     *
     * @param errorSlotSize the error slot size
     * @return this
     */
    public CommonConfiguration errorSlotSize(final int errorSlotSize)
    {
        this.errorSlotSize = errorSlotSize;
        return this;
    }

    /**
     * Sets the inbound max claim attempts.
     *
     * @param inboundMaxClaimAttempts the inbound max claim attempts
     * @return this
     *
     * @see CommonConfiguration#INBOUND_MAX_CLAIM_ATTEMPTS_PROPERTY
     */
    public CommonConfiguration inboundMaxClaimAttempts(final int inboundMaxClaimAttempts)
    {
        this.inboundMaxClaimAttempts = inboundMaxClaimAttempts;
        return this;
    }

    /**
     * Sets the outbound max claim attempts.
     *
     * @param outboundMaxClaimAttempts the outbound max claim attempts
     * @return this
     *
     * @see CommonConfiguration#OUTBOUND_MAX_CLAIM_ATTEMPTS_PROPERTY
     */
    public CommonConfiguration outboundMaxClaimAttempts(final int outboundMaxClaimAttempts)
    {
        this.outboundMaxClaimAttempts = outboundMaxClaimAttempts;
        return this;
    }

    /**
     * Sets the session's encoding buffer size. The session buffer is a buffer used by each Session to encode
     * messages via {@link uk.co.real_logic.fix_gateway.session.Session#send(uk.co.real_logic.fix_gateway.builder.MessageEncoder)}.
     *
     * @param bufferSize the session's encoding buffer size
     * @return this
     */
    public CommonConfiguration sessionBufferSize(final int bufferSize)
    {
        this.sessionBufferSize = bufferSize;
        return this;
    }

    public CommonConfiguration histogramPollPeriodInMs(final long histogramPollPeriodInMs)
    {
        this.histogramPollPeriodInMs = histogramPollPeriodInMs;
        return this;
    }

    public CommonConfiguration histogramLoggingFile(final String histogramLoggingFile)
    {
        this.histogramLoggingFile = histogramLoggingFile;
        return this;
    }

    public Aeron.Context aeronContext()
    {
        return aeronContext;
    }

    public boolean printErrorMessages()
    {
        return printErrorMessages;
    }

    public IdleStrategy errorPrinterIdleStrategy()
    {
        return errorPrinterIdleStrategy;
    }

    public SessionIdStrategy sessionIdStrategy()
    {
        return sessionIdStrategy;
    }

    public AuthenticationStrategy authenticationStrategy()
    {
        return authenticationStrategy;
    }

    public SessionCustomisationStrategy sessionCustomisationStrategy()
    {
        return sessionCustomisationStrategy;
    }

    public MessageValidationStrategy messageValidationStrategy()
    {
        return messageValidationStrategy;
    }

    public int monitoringBuffersLength()
    {
        return monitoringBuffersLength;
    }

    public String monitoringFile()
    {
        return monitoringFile;
    }

    public long replyTimeoutInMs()
    {
        return replyTimeoutInMs;
    }

    public int errorSlotSize()
    {
        return errorSlotSize;
    }

    public long histogramPollPeriodInMs()
    {
        return histogramPollPeriodInMs;
    }

    /**
     * If shared memory is available, use that as a temporary directory,
     * otherwise use the default temp directory
     *
     * @return the optimal temporary directory
     */
    public static String optimalTmpDirName()
    {
        if ("Linux".equalsIgnoreCase(System.getProperty("os.name")))
        {
            final File devShmDir = new File("/dev/shm");

            if (devShmDir.exists())
            {
                return devShmDir.getAbsolutePath();
            }
        }

        return IoUtil.tmpDirName();
    }

    public static IdleStrategy backoffIdleStrategy()
    {
        return new BackoffIdleStrategy(BACKOFF_SPINS, BACKOFF_YIELDS, 1, 1 << 20);
    }

    public int inboundMaxClaimAttempts()
    {
        return inboundMaxClaimAttempts;
    }

    public int outboundMaxClaimAttempts()
    {
        return outboundMaxClaimAttempts;
    }

    public int sessionBufferSize()
    {
        return sessionBufferSize;
    }

    public String histogramLoggingFile()
    {
        return histogramLoggingFile;
    }

    protected void conclude(final String fixSuffix)
    {
        if (monitoringFile() == null)
        {
            monitoringFile(getProperty(MONITORING_FILE_PROPERTY, String.format(DEFAULT_MONITORING_FILE, fixSuffix)));
        }

        if (histogramLoggingFile() == null)
        {
            histogramLoggingFile(getProperty(
                HISTOGRAM_LOGGING_FILE_PROPERTY, String.format(DEFAULT_HISTOGRAM_LOGGING_FILE, fixSuffix)));
        }
    }
}
