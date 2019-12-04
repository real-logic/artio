/*
 * Copyright 2015-2019 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.session;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.Verify;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.dictionary.generation.CodecUtil;
import uk.co.real_logic.artio.fields.RejectReason;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.lang.Integer.MIN_VALUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static uk.co.real_logic.artio.LogTag.FIX_MESSAGE;
import static uk.co.real_logic.artio.builder.Validation.CODEC_VALIDATION_DISABLED;
import static uk.co.real_logic.artio.builder.Validation.CODEC_VALIDATION_ENABLED;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_LONG;
import static uk.co.real_logic.artio.fields.RejectReason.*;
import static uk.co.real_logic.artio.library.SessionConfiguration.NO_RESEND_REQUEST_CHUNK_SIZE;
import static uk.co.real_logic.artio.messages.DisconnectReason.*;
import static uk.co.real_logic.artio.messages.MessageStatus.OK;
import static uk.co.real_logic.artio.messages.SessionState.*;
import static uk.co.real_logic.artio.session.DirectSessionProxy.NO_LAST_MSG_SEQ_NUM_PROCESSED;
import static uk.co.real_logic.artio.session.InternalSession.*;
import static uk.co.real_logic.artio.dictionary.SessionConstants.*;

/**
 * Stores information about the current state of a session - no matter whether outbound or inbound.
 * <p>
 * Should only be accessed on a single thread.
 *
 * <h1>State Transitions</h1>
 * <p>
 * Successful Login: CONNECTED -&gt; ACTIVE
 * Login with high sequence number: CONNECTED -&gt; AWAITING_RESEND
 * Login with low sequence number: CONNECTED -&gt; DISCONNECTED
 * Login with wrong credentials: CONNECTED -&gt; DISCONNECTED or CONNECTED -&gt; DISABLED
 * depending on authentication plugin
 * <p>
 * Successful Hijack: * -&gt; ACTIVE (same as regular login)
 * Hijack with high sequence number: * -&gt; AWAITING_RESEND (same as regular login)
 * Hijack with low sequence number: requestDisconnect the hijacker and leave main system ACTIVE
 * Hijack with wrong credentials: requestDisconnect the hijacker and leave main system ACTIVE
 * <p>
 * Successful resend: AWAITING_RESEND -&gt; ACTIVE
 * <p>
 * Send test request: ACTIVE -&gt; ACTIVE - but alter the timeout for the next expected heartbeat.
 * Successful Heartbeat: ACTIVE -&gt; ACTIVE - updates the timeout time.
 * Heartbeat Timeout: ACTIVE -&gt; DISCONNECTED
 * <p>
 * Logout request: ACTIVE -&gt; AWAITING_LOGOUT
 * Logout acknowledgement: AWAITING_LOGOUT -&gt; DISCONNECTED
 * <p>
 * Manual disable: * -&gt; DISABLED
 */
public class Session implements AutoCloseable
{
    public static final long UNKNOWN = -1;
    public static final long NO_LOGON_TIME = -1;

    static final short ACTIVE_VALUE = 3;
    static final short LOGGING_OUT_VALUE = 5;
    static final short LOGGING_OUT_AND_DISCONNECTING_VALUE = 6;
    static final short AWAITING_LOGOUT_VALUE = 7;
    static final short DISCONNECTING_VALUE = 8;

    private static final long NO_OPERATION = MIN_VALUE;
    static final long LIBRARY_DISCONNECTED = NO_OPERATION + 1;
    private static final int INITIAL_SEQUENCE_NUMBER = 1;

    /**
     * The proportion of the maximum heartbeat interval before you send your heartbeat
     */
    private static final double HEARTBEAT_PAUSE_FACTOR = 0.8;

    static final String TEST_REQ_ID = "TEST";
    private static final char[] TEST_REQ_ID_CHARS = TEST_REQ_ID.toCharArray();
    private static final int NO_LOGOUT_REJECT_REASON = -1;

    private final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();

    protected final long connectionId;
    protected final SessionIdStrategy sessionIdStrategy;
    protected final GatewayPublication publication;
    protected final MutableAsciiBuffer asciiBuffer;
    protected final int libraryId;
    protected final SessionProxy proxy;

    private final EpochClock epochClock;
    private final long sendingTimeWindowInMs;
    private final AtomicCounter receivedMsgSeqNo;
    private final AtomicCounter sentMsgSeqNo;
    private final long reasonableTransmissionTimeInMs;
    private final boolean enableLastMsgSeqNumProcessed;
    private final String beginString;

    private CompositeKey sessionKey;
    private SessionState state;
    // Used to trigger a disconnect if we don't receive a resend within expected timeout
    private boolean awaitingResend = INITIAL_AWAITING_RESEND;
    // Equivalent of receivedMsgSeqNo for resent messages
    private int lastResentMsgSeqNo = INITIAL_LAST_RESENT_MSG_SEQ_NO;
    // The last msg seq no before you send the next chunk of the resend request
    private int lastResendChunkMsgSeqNum = INITIAL_LAST_RESEND_CHUNK_MSG_SEQ_NUM;
    // The last msg seq no before you hit the end of the resend request
    private int endOfResendRequestRange = INITIAL_END_OF_RESEND_REQUEST_RANGE;

    private boolean awaitingHeartbeat = INITIAL_AWAITING_HEARTBEAT;

    private long id = UNKNOWN;
    private int lastReceivedMsgSeqNum;
    private int lastMsgSeqNumProcessed;
    private int lastSentMsgSeqNum;
    private int sequenceIndex;

    private long heartbeatIntervalInMs;
    private long nextRequiredInboundMessageTimeInMs;
    private long sendingHeartbeatIntervalInMs;
    private long nextRequiredHeartbeatTimeInMs;

    private String username;
    private String password;
    private String connectedHost;
    private int connectedPort;
    private long logonTime = NO_LOGON_TIME;
    private boolean closedResendInterval;
    private int resendRequestChunkSize;
    private boolean sendRedundantResendRequests;

    private boolean incorrectBeginString = false;

    private SessionLogonListener logonListener;

    private int logoutRejectReason = NO_LOGOUT_REJECT_REASON;

    public Session(
        final int heartbeatIntervalInS,
        final long connectionId,
        final EpochClock epochClock,
        final SessionState state,
        final SessionProxy proxy,
        final GatewayPublication publication,
        final SessionIdStrategy sessionIdStrategy,
        final long sendingTimeWindowInMs,
        final AtomicCounter receivedMsgSeqNo,
        final AtomicCounter sentMsgSeqNo,
        final int libraryId,
        final int initialSentSequenceNumber,
        final int sequenceIndex,
        final long reasonableTransmissionTimeInMs,
        final MutableAsciiBuffer asciiBuffer,
        final boolean enableLastMsgSeqNumProcessed,
        final String beginString)
    {
        Verify.notNull(epochClock, "clock");
        Verify.notNull(state, "session state");
        Verify.notNull(proxy, "session proxy");
        Verify.notNull(publication, "publication");
        Verify.notNull(receivedMsgSeqNo, "received MsgSeqNo counter");
        Verify.notNull(sentMsgSeqNo, "sent MsgSeqNo counter");
        Verify.notNull(beginString, "beginString");

        this.epochClock = epochClock;
        this.proxy = proxy;
        this.connectionId = connectionId;
        this.publication = publication;
        this.sessionIdStrategy = sessionIdStrategy;
        this.sendingTimeWindowInMs = sendingTimeWindowInMs;
        this.receivedMsgSeqNo = receivedMsgSeqNo;
        this.sentMsgSeqNo = sentMsgSeqNo;
        this.libraryId = libraryId;
        sequenceIndex(sequenceIndex);
        this.lastSentMsgSeqNum = initialSentSequenceNumber - 1;
        this.reasonableTransmissionTimeInMs = reasonableTransmissionTimeInMs;
        this.enableLastMsgSeqNumProcessed = enableLastMsgSeqNumProcessed;
        this.beginString = beginString;
        this.asciiBuffer = asciiBuffer;

        state(state);
        heartbeatIntervalInS(heartbeatIntervalInS);
        lastMsgSeqNumProcessed = this.enableLastMsgSeqNumProcessed ? 0 : NO_LAST_MSG_SEQ_NUM_PROCESSED;
    }

    // ---------- PUBLIC API ----------

    /**
     * Check if the session is connected to another session.
     *
     * @return true if the session is connected to another session, false otherwise.
     */
    public boolean isConnected()
    {
        final SessionState state = state();
        return state != CONNECTING && state != DISCONNECTED && state != DISABLED;
    }

    /**
     * Get the session's state.
     *
     * @return the session's state.
     */
    public SessionState state()
    {
        return state;
    }

    /**
     * Get whether the session is awaiting a resend / replay of messages.
     *
     * @return true iff the session is awaiting a resend / replay of messages.
     */
    public boolean awaitingResend()
    {
        return awaitingResend;
    }

    public boolean awaitingHeartbeat()
    {
        return awaitingHeartbeat;
    }

    public boolean closedResendInterval()
    {
        return closedResendInterval;
    }

    public int resendRequestChunkSize()
    {
        return resendRequestChunkSize;
    }

    public boolean sendRedundantResendRequests()
    {
        return sendRedundantResendRequests;
    }

    /**
     * Get the username associated with this session.
     *
     * @return the username associated with this session.
     */
    public String username()
    {
        return username;
    }

    /**
     * Get the password associated with this session.
     *
     * @return the password associated with this session.
     */
    public String password()
    {
        return password;
    }

    /**
     * Get the sequence number of the last message to be sent from this session.
     *
     * @return the sequence number of the last message to be sent from this session.
     */
    public int lastSentMsgSeqNum()
    {
        return lastSentMsgSeqNum;
    }

    /**
     * Get the sequence number of the last message to be received by this session.
     *
     * @return the sequence number of the last message to be received by this session.
     */
    public int lastReceivedMsgSeqNum()
    {
        return lastReceivedMsgSeqNum;
    }

    /**
     * Get the heartbeat interval for this session in milliseconds. This can be configured locally
     * or agreed by the logon process.
     *
     * @return the heartbeat interval for this session in milliseconds.
     */
    public long heartbeatIntervalInMs()
    {
        return heartbeatIntervalInMs;
    }

    /**
     * Get the address of the remote host that your session is connected to.
     *
     * @return the address of the remote host that your session is connected to.
     * @see Session#connectedPort()
     */
    public String connectedHost()
    {
        return connectedHost;
    }

    /**
     * Get the id of the connection associated with this session. Sessions always
     * have a connection id.
     *
     * @return the id of the connection associated with this session.
     * @see Session#id()
     */
    public long connectionId()
    {
        return connectionId;
    }

    /**
     * Get the id of this session. If the session hasn't logged in yet, this
     * will return <code>Session.UNKNOWN</code>.
     *
     * @return the id of the session if known.
     * @see Session#UNKNOWN
     */
    public long id()
    {
        return id;
    }

    /**
     * Get the uniquely identifying key of the session. This contains any comp, sub or location
     * ids used to uniquely identify the session.
     *
     * @return the uniquely identifying key of the session
     */
    public CompositeKey compositeKey()
    {
        return sessionKey;
    }

    /**
     * Get the port of the remote host that your session is connected to.
     *
     * @return the port of the remote host that your session is connected to.
     * @see Session#connectedHost()
     */
    public int connectedPort()
    {
        return connectedPort;
    }

    /**
     * Sends a logout message and puts the session into the awaiting logout state.
     *
     * This method will eventually also disconnect the Session, but it won't disconnect the session until you
     * receive a logout message from your counter-party. That's the difference between this and
     * <code>logoutAndDisconnect</code> - that method just disconnects you as soon as possible.
     *
     * @return the position of the sent message
     * @see Session#logoutAndDisconnect()
     */
    public long startLogout()
    {
        final long position = sendLogout();
        state(position < 0 ? LOGGING_OUT : AWAITING_LOGOUT);
        return position;
    }

    /**
     * Request the session be disconnected.
     *
     * @see Session#logoutAndDisconnect()
     * @return the position within the Aeron stream where the disconnect is encoded.
     */
    public long requestDisconnect()
    {
        return requestDisconnect(APPLICATION_DISCONNECT);
    }

    private long requestDisconnect(final DisconnectReason reason)
    {
        long position = NO_OPERATION;
        if (state() != DISCONNECTED)
        {
            position = proxy.sendRequestDisconnect(connectionId, reason);
            state(position < 0 ? DISCONNECTING : DISCONNECTED);
        }

        return position;
    }

    /**
     * Send a logout message and immediately disconnect the session. You should normally use
     * the <code>startLogout</code> method and not this one.
     * <p>
     * This disconnects the session faster than <code>startLogout</code>. This approach does not linger
     * the Session, awaiting for the disconnect and it's possible that your counter-party misses the logout
     * message. This should only be used when you want to rapidly disconnect the session and are willing
     * to take the risk that the logout message is not received.
     *
     * @see Session#startLogout()
     * @return the position within the Aeron stream where the disconnect is encoded.
     */
    public long logoutAndDisconnect()
    {
        return logoutAndDisconnect(APPLICATION_DISCONNECT);
    }

    private long logoutAndDisconnect(final DisconnectReason reason)
    {
        long position = NO_OPERATION;
        if (state() != DISCONNECTED)
        {
            position = sendLogout();
            if (position < 0)
            {
                state(LOGGING_OUT_AND_DISCONNECTING);
            }
            else
            {
                position = requestDisconnect(reason);
            }
        }

        return position;
    }

    /**
     * Prepare header with session state
     *
     * @param header the encoder header
     * @return the sent sequence number for the header
     */
    public int prepare(final SessionHeaderEncoder header)
    {
        final int sentSeqNum = newSentSeqNum();
        header
            .msgSeqNum(sentSeqNum)
            .sendingTime(timestampEncoder.buffer(), timestampEncoder.encode(time()));

        if (enableLastMsgSeqNumProcessed)
        {
            header.lastMsgSeqNumProcessed(lastMsgSeqNumProcessed);
        }

        if (!header.hasSenderCompID())
        {
            sessionIdStrategy.setupSession(sessionKey, header);
        }
        return sentSeqNum;
    }

    /**
     * Send a message on this session.
     *
     * @param encoder the encoder of the message to be sent
     * @return the position in the stream that corresponds to the end of this message or a negative
     * number indicating an error status.
     * @throws IndexOutOfBoundsException if the encoded message is too large, if this happens consider
     *                                   increasing {@link CommonConfiguration#sessionBufferSize(int)}
     */
    public long send(final Encoder encoder)
    {
        validateCanSendMessage();

        final int sentSeqNum = prepare(encoder.header());

        final long result = encoder.encode(asciiBuffer, 0);
        final int length = Encoder.length(result);
        final int offset = Encoder.offset(result);

        return send(asciiBuffer, offset, length, sentSeqNum, encoder.messageType());
    }

    /**
     * Send a message on this session.
     *
     * @param messageBuffer the buffer with the FIX message in to send
     * @param offset the offset within the messageBuffer where the message starts
     * @param length the length of the message within the messageBuffer
     * @param seqNum the sequence number of the sent message
     * @param messageType the long encoded message type.
     * @return the position in the stream that corresponds to the end of this message or a negative
     * number indicating an error status.
     */
    public long send(
        final DirectBuffer messageBuffer, final int offset, final int length, final int seqNum, final long messageType)
    {
        validateCanSendMessage();

        final long position = publication.saveMessage(
            messageBuffer, offset, length, libraryId, messageType, id(), sequenceIndex(), connectionId, OK, seqNum);

        if (position > 0)
        {
            lastSentMsgSeqNum(seqNum, position);

            DebugLogger.log(FIX_MESSAGE, "Sent %s %n", messageBuffer, offset, length);
        }

        return position;
    }

    /**
     * Check if the session is in a state where it can send a message.
     *
     * @return true if the session is in a state where it can send a message, false otherwise.
     */
    public boolean canSendMessage()
    {
        return state == ACTIVE;
    }

    /**
     * Reset the sequence number, so that the specified sequence number will be the sequence
     * number of the next message. This sends a sequence reset message and can thus only be
     * used to increase the sequence number of the session.
     * <p>
     * If you want to reset the sequence number back to 1 you should use
     * {@link #resetSequenceNumbers()}.
     *
     * @param nextSentMessageSequenceNumber the new sequence number of the next message to be
     *                                      sent.
     * @return the position in the stream that corresponds to the end of this message.
     */
    public long sendSequenceReset(
        final int nextSentMessageSequenceNumber)
    {
        nextSequenceIndex();
        final long position = proxy.sendSequenceReset(
            lastSentMsgSeqNum, nextSentMessageSequenceNumber, sequenceIndex(), lastMsgSeqNumProcessed);
        lastSentMsgSeqNum(nextSentMessageSequenceNumber - 1, position);

        return position;
    }

    /**
     * Acts like {@link #sendSequenceReset(int, int)} but also resets the received sequence number.
     *
     * @param nextSentMessageSequenceNumber the new sequence number of the next message to be
     *                                      sent.
     * @param nextReceivedMessageSequenceNumber the new sequence number of the next message to be
     *                                          received.
     * @return the position in the stream that corresponds to the end of this message.
     */
    public long sendSequenceReset(
        final int nextSentMessageSequenceNumber,
        final int nextReceivedMessageSequenceNumber)
    {
        final long position = sendSequenceReset(nextSentMessageSequenceNumber);
        lastReceivedMsgSeqNum(nextReceivedMessageSequenceNumber - 1);

        return position;
    }

    private void nextSequenceIndex()
    {
        sequenceIndex++;
    }

    /**
     * Resets both the receiver and sender sequence numbers of this session. This is equivalent to
     * sending a Logon message with ResetSeqNum flag set to Y.
     * <p>
     * If you want to send a sequence reset message then you should use {@link #sendSequenceReset(int, int)}.
     *
     * @return the position in the stream that corresponds to the end of this message.
     */
    public long resetSequenceNumbers()
    {
        final int sentSeqNum = 1;
        final int heartbeatIntervalInS = (int)MILLISECONDS.toSeconds(heartbeatIntervalInMs);
        nextSequenceIndex();
        final long position = proxy.sendLogon(
            sentSeqNum,
            heartbeatIntervalInS,
            username(),
            password(),
            true,
            sequenceIndex(),
            lastMsgSeqNumProcessed);
        lastSentMsgSeqNum(sentSeqNum, position);

        return position;
    }

    public boolean isActive()
    {
        return state == ACTIVE;
    }

    public boolean isAcceptor()
    {
        return false;
    }

    /**
     * Get the current sequence index. This is a number that increments everytime the
     * sequence numbers get reset. In combination with the sequence numbers it provides
     * a monotonically increasing sequence.
     *
     * @return the current sequence index
     */
    public int sequenceIndex()
    {
        return sequenceIndex;
    }

    /**
     * Close the session object and release its resources.
     * <p>
     * API users should never have to call this method.
     */
    public void close()
    {
        sentMsgSeqNo.close();
        receivedMsgSeqNo.close();
    }

    // ---------- Event Handlers & Logic ----------

    Action onInvalidFixDisconnect()
    {
        return Pressure.apply(requestDisconnect(DisconnectReason.INVALID_FIX_MESSAGE));
    }

    public void onDisconnect()
    {
        logoutRejectReason = NO_LOGOUT_REJECT_REASON;
        state(DISCONNECTED);
    }

    private void lastSentMsgSeqNum(final int sentSeqNum, final long position)
    {
        if (position >= 0)
        {
            lastSentMsgSeqNum(sentSeqNum);
        }
    }

    private void validateCanSendMessage()
    {
        if (!canSendMessage())
        {
            throw new IllegalStateException(
                String.format("Session isn't active it's %s, and thus can't send a message", state));
        }
    }

    Action onMessage(
        final int msgSeqNo,
        final char[] msgType,
        final long sendingTime,
        final long origSendingTime,
        final boolean isPossDupOrResend,
        final boolean possDup)
    {
        return onMessage(msgSeqNo, msgType, msgType.length, sendingTime, origSendingTime, isPossDupOrResend, possDup);
    }

    Action onMessage(
        final int msgSeqNo,
        final char[] msgType,
        final int msgTypeLength,
        final long sendingTime,
        final long origSendingTime,
        final boolean isPossDupOrResend,
        final boolean possDup)
    {
        if (state() == SessionState.CONNECTED)
        {
            // Disconnect if the first message isn't a logon message
            return Pressure.apply(requestDisconnect(FIRST_MESSAGE_NOT_LOGON));
        }
        else
        {
            final long time = time();
            final Action action = validateRequiredFieldsAndCodec(
                msgSeqNo, time, msgType, msgTypeLength, sendingTime, origSendingTime, possDup);
            if (action != null)
            {
                return action;
            }

            return checkSeqNoChange(msgSeqNo, time, isPossDupOrResend);
        }
    }

    private Action validateRequiredFieldsAndCodec(
        final int msgSeqNo,
        final long time,
        final char[] msgType,
        final int msgTypeLength,
        final long sendingTime,
        final long origSendingTime,
        final boolean possDup)
    {
        if (msgSeqNo == MISSING_INT)
        {
            final int sentSeqNum = newSentSeqNum();
            return checkPositionAndDisconnect(
                proxy.sendReceivedMessageWithoutSequenceNumber(sentSeqNum, sequenceIndex(), lastMsgSeqNumProcessed),
                MSG_SEQ_NO_MISSING);
        }

        if (CODEC_VALIDATION_ENABLED)
        {
            final Action validationResult = validateCodec(time, msgSeqNo, msgType, msgTypeLength, sendingTime,
                origSendingTime, possDup);
            if (validationResult != null)
            {
                return validationResult;
            }
        }

        return null;
    }

    // returns final state of session after validation or null if further processing required
    private Action validateCodec(
        final long time,
        final int msgSeqNum,
        final char[] msgType,
        final int msgTypeLength,
        final long sendingTime,
        final long origSendingTime,
        final boolean possDup)
    {
        if (possDup)
        {
            if (origSendingTime == UNKNOWN)
            {
                return checkPosition(proxy.sendReject(
                    newSentSeqNum(),
                    msgSeqNum,
                    MISSING_INT,
                    msgType,
                    msgTypeLength,
                    REQUIRED_TAG_MISSING.representation(),
                    sequenceIndex(),
                    lastMsgSeqNumProcessed));
            }
            else if (origSendingTime > sendingTime)
            {
                return rejectDueToSendingTime(msgSeqNum, msgType, msgTypeLength);
            }
        }

        if ((sendingTime < time - sendingTimeWindowInMs) || (sendingTime > time + sendingTimeWindowInMs))
        {
            final Action action = rejectDueToSendingTime(msgSeqNum, msgType, msgTypeLength);
            if (action != ABORT)
            {
                logoutRejectReason(RejectReason.SENDINGTIME_ACCURACY_PROBLEM.representation());
                logoutAndDisconnect(INVALID_SENDING_TIME);
            }

            return action;
        }

        return null;
    }

    private Action checkSeqNoChange(final int msgSeqNum, final long time, final boolean isPossDupOrResend)
    {
        if (awaitingResend)
        {
            if (msgSeqNum == endOfResendMsgSeqNum())
            {
                awaitingResend = false;
                lastResendChunkMsgSeqNum = 0;
                lastResentMsgSeqNo = 0;
                endOfResendRequestRange = 0;
            }
            else if (msgSeqNum == lastResendChunkMsgSeqNum)
            {
                final Action action = checkPosition(sendResendRequest(
                    msgSeqNum + 1, // Effectively begin
                    endOfResendMsgSeqNum()));   // Effectively ideal end pre chunking
                if (action == CONTINUE)
                {
                    lastResentMsgSeqNo = msgSeqNum;
                }

                return action;
            }
            else if (msgSeqNum > endOfResendRequestRange)
            {
                if (sendRedundantResendRequests)
                {
                    return Pressure.apply(sendResendRequest(lastResendChunkMsgSeqNum, msgSeqNum));
                }
                else
                {
                    return checkNormalSeqNoChange(msgSeqNum, time, isPossDupOrResend);
                }
            }
            else
            {
                lastResentMsgSeqNo = msgSeqNum;
            }
        }
        else
        {
            return checkNormalSeqNoChange(msgSeqNum, time, isPossDupOrResend);
        }

        return CONTINUE;
    }

    private Action checkNormalSeqNoChange(final int msgSeqNum, final long time, final boolean isPossDupOrResend)
    {
        final int expectedSeqNo = expectedReceivedSeqNum();
        if (expectedSeqNo == msgSeqNum)
        {
            incNextReceivedInboundMessageTime(time);
            lastReceivedMsgSeqNum(msgSeqNum);
        }
        else if (expectedSeqNo < msgSeqNum)
        {
            return requestResend(expectedSeqNo, msgSeqNum);
        }
        else if (/* expectedSeqNo > msgSeqNo && */ !isPossDupOrResend)
        {
            return msgSeqNumTooLow(msgSeqNum, expectedSeqNo);
        }
        return CONTINUE;
    }

    private int endOfResendMsgSeqNum()
    {
        return endOfResendRequestRange;
    }

    private Action requestResend(final int expectedSeqNo, final int receivedMsgSeqNo)
    {
        final long position = sendResendRequest(expectedSeqNo, receivedMsgSeqNo - 1);
        if (position >= 0)
        {
            awaitingResend = true;
            lastResentMsgSeqNo = expectedSeqNo - 1;
            lastReceivedMsgSeqNum = receivedMsgSeqNo;
            endOfResendRequestRange = receivedMsgSeqNo - 1;
        }
        return checkPosition(position);
    }

    private long sendResendRequest(final int expectedSeqNo, final int receivedMsgSeqNo)
    {
        // Cap at a chunk size if specified, otherwise send 0 to indicate infinity or the receivedMsgSeqNo
        final boolean chunkedResend = resendRequestChunkSize != NO_RESEND_REQUEST_CHUNK_SIZE;
        final int cappedEndSeqNo = chunkedResend ? expectedSeqNo + resendRequestChunkSize - 1 : receivedMsgSeqNo;

        final int endSeqNo;
        if (cappedEndSeqNo < receivedMsgSeqNo)
        {
            endSeqNo = cappedEndSeqNo;
        }
        else
        {
            endSeqNo = closedResendInterval ? receivedMsgSeqNo : 0;
        }

        final long position = proxy.sendResendRequest(
            newSentSeqNum(),
            expectedSeqNo,
            endSeqNo,
            sequenceIndex(),
            lastMsgSeqNumProcessed);

        if (position > 0 && chunkedResend)
        {
            lastResendChunkMsgSeqNum = cappedEndSeqNo;
        }

        return position;
    }

    private Action msgSeqNumTooLow(final int msgSeqNo, final int expectedSeqNo)
    {
        return checkPositionAndDisconnect(
            proxy.sendLowSequenceNumberLogout(
                newSentSeqNum(), expectedSeqNo, msgSeqNo, sequenceIndex(), lastMsgSeqNumProcessed),
            MSG_SEQ_NO_TOO_LOW);
    }

    private Action checkPosition(final long position)
    {
        if (position < 0)
        {
            return ABORT;
        }
        else
        {
            lastSentMsgSeqNum(newSentSeqNum());
            return CONTINUE;
        }
    }

    private Action rejectDueToSendingTime(final int msgSeqNo, final char[] msgType, final int msgTypeLength)
    {
        return checkPosition(proxy.sendReject(
            newSentSeqNum(),
            msgSeqNo,
            SENDING_TIME,
            msgType,
            msgTypeLength,
            SENDINGTIME_ACCURACY_PROBLEM.representation(),
            sequenceIndex(),
            lastMsgSeqNumProcessed));
    }

    private void incNextReceivedInboundMessageTime(final long time)
    {
        this.nextRequiredInboundMessageTimeInMs = time + heartbeatIntervalInMs() + reasonableTransmissionTimeInMs;
    }

    Action onLogon(
        final int heartbeatInterval,
        final int msgSeqNum,
        final long sendingTime,
        final long origSendingTime,
        final String username,
        final String password,
        final boolean isPossDupOrResend,
        final boolean resetSeqNumFlag,
        final boolean possDup)
    {
        // We aren't checking CODEC_VALIDATION_ENABLED here because these are required values in order to
        // have a stable FIX connection.
        Action action = validateOrRejectHeartbeat(heartbeatInterval);
        if (action != null)
        {
            return action;
        }

        action = validateOrRejectSendingTime(sendingTime);
        if (action != null)
        {
            return action;
        }

        final long logonTime = sendingTime(sendingTime, origSendingTime);

        if (resetSeqNumFlag)
        {
            return onResetSeqNumLogon(heartbeatInterval, username, password, logonTime, msgSeqNum);
        }

        if (state() == initialState())
        {
            // Initial income connection logic
            final int expectedMsgSeqNo = expectedReceivedSeqNum();
            if (expectedMsgSeqNo == msgSeqNum)
            {
                // Send outbound logon message and check if backpressured on the outward client side.
                action = respondToLogon(heartbeatInterval);
                if (action == ABORT)
                {
                    return ABORT;
                }

                // Don't configure this session as active until successful outbound publication
                setupCompleteLogonState(logonTime, heartbeatInterval, msgSeqNum, username, password, time());
                lastReceivedMsgSeqNum(msgSeqNum);

                return CONTINUE;
            }
            else if (expectedMsgSeqNo < msgSeqNum)
            {
                // If their sequence number is higher than expected, we still accept the logon.
                action = respondToLogon(heartbeatInterval);
                if (action == ABORT)
                {
                    return ABORT;
                }

                final boolean requestSeqNumReset = proxy.seqNumResetRequested();
                if (requestSeqNumReset) // if we requested sequence number reset then do not await for replay
                {
                    lastReceivedMsgSeqNum = 0; // TODO: should this not be msgSeqNum?
                    setupCompleteLogonStateReset(logonTime, heartbeatInterval, username, password, time());

                    return CONTINUE;
                }
                else
                {
                    setupCompleteLogonState(logonTime, heartbeatInterval, msgSeqNum, username, password, time());
                    action = requestResend(expectedMsgSeqNo, msgSeqNum);

                    return action;
                }
            }
            else // (msgSeqNo < expectedMsgSeqNo)
            {
                return msgSeqNumTooLow(msgSeqNum, expectedMsgSeqNo);
            }
        }
        else
        {
            // You've received a logon and you weren't expecting one and it hasn't got the resetSeqNumFlag set
            return onMessage(
                msgSeqNum, LOGON_MESSAGE_TYPE_CHARS, sendingTime, origSendingTime, isPossDupOrResend, possDup);
        }
    }

    protected Action respondToLogon(final int heartbeatInterval)
    {
        return replyToLogon(heartbeatInterval);
    }

    protected SessionState initialState()
    {
        return SessionState.CONNECTED;
    }

    // Always resets the sequence number to 1
    private Action onResetSeqNumLogon(
        final int heartbeatInterval,
        final String username,
        final String password,
        final long logonTime,
        final int msgSeqNo)
    {
        // if we have just received a reset request and not a response to one we just sent.
        if (lastSentMsgSeqNum() != INITIAL_SEQUENCE_NUMBER)
        {
            final int logonSequenceIndex = isInitialRequest() ? sequenceIndex() : sequenceIndex() + 1;
            final long position = proxy.sendLogon(INITIAL_SEQUENCE_NUMBER, heartbeatInterval,
                null,
                null,
                true,
                logonSequenceIndex, lastMsgSeqNumProcessed);
            if (position < 0)
            {
                return ABORT;
            }

            lastSentMsgSeqNum(INITIAL_SEQUENCE_NUMBER);
            lastReceivedMsgSeqNum(msgSeqNo);
        }
        else
        {
            lastReceivedMsgSeqNumOnly(msgSeqNo);
        }

        // logon time becomes time of the confirmation message.
        setupCompleteLogonStateReset(logonTime, heartbeatInterval, username, password, time());

        return CONTINUE;
    }

    private void setupCompleteLogonState(
        final long logonTime,
        final int heartbeatInterval,
        final int msgSeqNum,
        final String username,
        final String password,
        final long currentTime)
    {
        if (msgSeqNum == INITIAL_SEQUENCE_NUMBER)
        {
            logonTime(logonTime);
        }
        setupLogonState(heartbeatInterval, username, password, currentTime);
    }

    private void setupCompleteLogonStateReset(
        final long logonTime,
        final int heartbeatInterval,
        final String username,
        final String password,
        final long currentTime)
    {
        logonTime(logonTime);
        setupLogonState(heartbeatInterval, username, password, currentTime);
    }

    private void setupLogonState(
        final int heartbeatInterval, final String username, final String password, final long currentTime)
    {
        incNextReceivedInboundMessageTime(currentTime);
        heartbeatIntervalInS(heartbeatInterval);
        state(ACTIVE);
        username(username);
        password(password);

        if (logonListener != null)
        {
            logonListener.onLogon(this);
        }
    }

    public void setupSession(final long sessionId, final CompositeKey sessionKey)
    {
        id(sessionId);
        this.sessionKey = sessionKey;
        proxy.setupSession(sessionId, sessionKey);
    }

    private Action replyToLogon(final int heartbeatInterval)
    {
        return checkPosition(proxy.sendLogon(
            newSentSeqNum(), heartbeatInterval, null, null, false, sequenceIndex(), lastMsgSeqNumProcessed));
    }

    private Action validateOrRejectSendingTime(final long sendingTime)
    {
        if (CODEC_VALIDATION_DISABLED && sendingTime == MISSING_LONG)
        {
            return null;
        }

        final long time = time();
        if ((sendingTime < (time + sendingTimeWindowInMs) && sendingTime > (time - sendingTimeWindowInMs)))
        {
            return null;
        }

        return checkPositionAndDisconnect(
            proxy.sendRejectWhilstNotLoggedOn(
                newSentSeqNum(), SENDINGTIME_ACCURACY_PROBLEM, sequenceIndex(), lastMsgSeqNumProcessed),
            INVALID_SENDING_TIME);
    }

    private Action validateOrRejectHeartbeat(final int heartbeatInterval)
    {
        if (heartbeatInterval < 0)
        {
            return checkPositionAndDisconnect(
                proxy.sendNegativeHeartbeatLogout(newSentSeqNum(), sequenceIndex(), lastMsgSeqNumProcessed),
                NEGATIVE_HEARTBEAT_INTERVAL);
        }
        else
        {
            return null;
        }
    }

    private Action checkPositionAndDisconnect(final long position, final DisconnectReason reason)
    {
        final Action action = checkPosition(position);
        if (action != ABORT)
        {
            requestDisconnect(reason);
        }

        return action;
    }

    private boolean isInitialRequest()
    {
        return 0 == lastReceivedMsgSeqNum();
    }

    Action onLogout(
        final int msgSeqNo,
        final long sendingTime,
        final long origSendingTime,
        final boolean possDup)
    {
        final long time = time();
        final Action action = validateRequiredFieldsAndCodec(
            msgSeqNo, time,
            LOGON_MESSAGE_TYPE_CHARS,
            LOGON_MESSAGE_TYPE_CHARS.length,
            sendingTime,
            origSendingTime,
            possDup);
        if (action == ABORT)
        {
            return ABORT;
        }

        lastReceivedMsgSeqNum(msgSeqNo);
        if (state() == AWAITING_LOGOUT)
        {
            requestDisconnect(LOGOUT);
        }
        else
        {
            logoutAndDisconnect(LOGOUT);
        }

        return CONTINUE;
    }

    Action onTestRequest(
        final int msgSeqNo,
        final char[] testReqId,
        final int testReqIdLength,
        final long sendingTime,
        final long origSendingTime,
        final boolean isPossDupOrResend,
        final boolean possDup)
    {
        if (msgSeqNo == expectedReceivedSeqNum())
        {
            final int sentSeqNum = newSentSeqNum();
            final long position = proxy.sendHeartbeat(
                sentSeqNum, testReqId, testReqIdLength, sequenceIndex(), lastMsgSeqNumProcessed);
            if (position < 0)
            {
                return ABORT;
            }
            else
            {
                lastSentMsgSeqNum(sentSeqNum);
            }
        }

        return onMessage(
            msgSeqNo, TEST_REQUEST_MESSAGE_TYPE_CHARS, sendingTime, origSendingTime, isPossDupOrResend, possDup);
    }

    Action onSequenceReset(
        final int msgSeqNo,
        final int newSeqNo,
        final boolean gapFillFlag,
        final boolean possDupFlag)
    {
        if (!gapFillFlag)
        {
            return applySequenceReset(msgSeqNo, newSeqNo);
        }
        else if (newSeqNo > msgSeqNo)
        {
            return onGapFill(msgSeqNo, newSeqNo, possDupFlag);
        }
        else
        {
            return applySequenceReset(msgSeqNo, newSeqNo);
        }
    }

    private Action applySequenceReset(final int receivedMsgSeqNo, final int newSeqNo)
    {
        final int expectedMsgSeqNo = expectedReceivedSeqNum();

        if (newSeqNo > expectedMsgSeqNo)
        {
            lastReceivedMsgSeqNum(newSeqNo - 1);
        }
        else if (newSeqNo < expectedMsgSeqNo)
        {
            return checkPosition(proxy.sendReject(
                newSentSeqNum(),
                receivedMsgSeqNo,
                NEW_SEQ_NO,
                SEQUENCE_RESET_MESSAGE_TYPE_CHARS,
                SEQUENCE_RESET_MESSAGE_TYPE_CHARS.length,
                RejectReason.VALUE_IS_INCORRECT.representation(),
                sequenceIndex(),
                lastMsgSeqNumProcessed));
        }

        return CONTINUE;
    }

    private Action onGapFill(final int receivedMsgSeqNo, final int newSeqNo, final boolean possDupFlag)
    {
        final int expectedMsgSeqNo = awaitingResend ? lastResentMsgSeqNo + 1 : expectedReceivedSeqNum();
        // The gapfill has the wrong sequence number.
        if (receivedMsgSeqNo > expectedMsgSeqNo)
        {
            final Action action = checkPosition(sendResendRequest(expectedMsgSeqNo, receivedMsgSeqNo - 1));
            if (action != ABORT)
            {
                if (awaitingResend)
                {
                    lastResentMsgSeqNo = newSeqNo - 1;
                }
                else
                {
                    lastReceivedMsgSeqNum(newSeqNo - 1);
                }
            }
            return action;
        }
        else if (receivedMsgSeqNo < expectedMsgSeqNo)
        {
            // Ignore the gapfill if it's a possibly a duplicate
            if (!possDupFlag)
            {
                return msgSeqNumTooLow(receivedMsgSeqNo, expectedMsgSeqNo);
            }
        }
        else // receivedMsgSeqNo == expectedMsgSeqNo
        {
            if (awaitingResend)
            {
                // A Resend Request would have put it in the AWAITING_RESEND state, we're now active again.
                if (lastReceivedMsgSeqNum <= newSeqNo)
                {
                    awaitingResend = false;
                    lastResentMsgSeqNo = 0;
                    lastResendChunkMsgSeqNum = 0;
                    endOfResendRequestRange = 0;
                }
                else
                {
                    if (newSeqNo == lastResendChunkMsgSeqNum)
                    {
                        final Action action = checkPosition(sendResendRequest(
                            newSeqNo,
                            endOfResendMsgSeqNum()));
                        if (action == CONTINUE)
                        {
                            lastResentMsgSeqNo = newSeqNo - 1;
                        }

                        return action;
                    }

                    lastResentMsgSeqNo = newSeqNo - 1;
                }
            }
            else
            {
                lastReceivedMsgSeqNum(newSeqNo - 1);
            }

        }

        return CONTINUE;
    }

    Action onReject(
        final int msgSeqNo,
        final long sendingTime,
        final long origSendingTime,
        final boolean isPossDupOrResend,
        final boolean possDup)
    {
        return onMessage(msgSeqNo, REJECT_MESSAGE_TYPE_CHARS, sendingTime, origSendingTime, isPossDupOrResend,
            possDup);
    }

    boolean onBeginString(final char[] value, final int length, final boolean isLogon)
    {
        final boolean isValid = CodecUtil.equals(value, beginString, length);
        if (!isValid)
        {
            if (!isLogon)
            {
                final int sentMsgSeqNum = newSentSeqNum();
                final long position = proxy.sendIncorrectBeginStringLogout(
                    sentMsgSeqNum, sequenceIndex(), lastMsgSeqNumProcessed);
                if (position < 0)
                {
                    incorrectBeginString = true;
                    state(DISCONNECTING);
                    return false;
                }
                else
                {
                    lastSentMsgSeqNum(sentMsgSeqNum);
                }
            }

            requestDisconnect(INCORRECT_BEGIN_STRING);
        }

        return isValid;
    }

    private void incNextHeartbeatTime()
    {
        nextRequiredHeartbeatTimeInMs = time() + sendingHeartbeatIntervalInMs;
    }

    private long sendLogout()
    {
        final int sentSeqNum = newSentSeqNum();
        final long position = (logoutRejectReason == NO_LOGOUT_REJECT_REASON) ?
            proxy.sendLogout(sentSeqNum, sequenceIndex(), lastMsgSeqNumProcessed) :
            proxy.sendLogout(sentSeqNum, sequenceIndex(), logoutRejectReason, lastMsgSeqNumProcessed);
        if (position >= 0)
        {
            lastSentMsgSeqNum(sentSeqNum);
        }

        return position;
    }

    // ---------- Setters ----------

    private void heartbeatIntervalInS(final int heartbeatIntervalInS)
    {
        this.heartbeatIntervalInMs = SECONDS.toMillis((long)heartbeatIntervalInS);

        final long time = time();
        incNextReceivedInboundMessageTime(time);
        sendingHeartbeatIntervalInMs = (long)(heartbeatIntervalInMs * HEARTBEAT_PAUSE_FACTOR);
        nextRequiredHeartbeatTimeInMs = time + sendingHeartbeatIntervalInMs;
    }

    protected Session state(final SessionState state)
    {
        this.state = state;
        return this;
    }

    public Session id(final long id)
    {
        this.id = id;
        return this;
    }

    protected long time()
    {
        return epochClock.time();
    }

    // Also checks the sequence index
    public Session lastReceivedMsgSeqNum(final int lastReceivedMsgSeqNum)
    {
        if (this.lastReceivedMsgSeqNum > lastReceivedMsgSeqNum)
        {
            nextSequenceIndex();
        }

        lastReceivedMsgSeqNumOnly(lastReceivedMsgSeqNum);

        return this;
    }

    // Does not check the sequence index
    private void lastReceivedMsgSeqNumOnly(final int value)
    {
        this.lastReceivedMsgSeqNum = value;
        receivedMsgSeqNo.setOrdered(value);
    }

    int expectedReceivedSeqNum()
    {
        return lastReceivedMsgSeqNum + 1;
    }

    int newSentSeqNum()
    {
        return lastSentMsgSeqNum + 1;
    }

    public int lastSentMsgSeqNum(final int lastSentMsgSeqNum)
    {
        this.lastSentMsgSeqNum = lastSentMsgSeqNum;
        sentMsgSeqNo.setOrdered(lastSentMsgSeqNum);
        incNextHeartbeatTime();

        return lastSentMsgSeqNum;
    }

    private void incReceivedSeqNum()
    {
        lastReceivedMsgSeqNum++;
        receivedMsgSeqNo.increment();
    }

    public long logonTime()
    {
        return this.logonTime;
    }

    public boolean hasLogonTime()
    {
        return logonTime != NO_LOGON_TIME;
    }

    Action onInvalidMessage(
        final int refSeqNum,
        final int refTagId,
        final char[] refMsgType,
        final int refMsgTypeLength,
        final int rejectReason)
    {
        final Action action = checkPosition(proxy.sendReject(
            newSentSeqNum(),
            refSeqNum,
            refTagId,
            refMsgType,
            refMsgTypeLength,
            rejectReason,
            sequenceIndex(),
            lastMsgSeqNumProcessed));

        if (action != ABORT)
        {
            incReceivedSeqNum();
        }

        return action;
    }

    Action onHeartbeat(
        final int msgSeqNum,
        final char[] testReqID,
        final int testReqIDLength,
        final long sendingTime,
        final long origSendingTime,
        final boolean isPossDupOrResend, final boolean possDup)
    {
        if (awaitingHeartbeat && CodecUtil.equals(testReqID, TEST_REQ_ID_CHARS, testReqIDLength))
        {
            awaitingHeartbeat = false;
        }

        return onMessage(
            msgSeqNum, HEARTBEAT_MESSAGE_TYPE_CHARS, sendingTime, origSendingTime, isPossDupOrResend, possDup);
    }

    Action onInvalidMessageType(final int msgSeqNum, final char[] msgType, final int msgTypeLength)
    {
        return checkPosition(proxy.sendReject(
            newSentSeqNum(),
            msgSeqNum,
            MISSING_INT,
            msgType,
            msgTypeLength,
            INVALID_MSGTYPE.representation(),
            sequenceIndex(),
            lastMsgSeqNumProcessed));
    }

    public String toString()
    {
        return getClass().getSimpleName() + "{" +
               "connectionId=" + connectionId +
               ", sessionId=" + id +
               ", state=" + state +
               ", sequenceIndex=" + sequenceIndex +
               ", lastReceivedMsgSeqNum=" + lastReceivedMsgSeqNum +
               ", lastSentMsgSeqNum=" + lastSentMsgSeqNum +
               '}';
    }

    void disable()
    {
        state(SessionState.DISABLED);
        close();
    }

    int poll(final long time)
    {
        final short state = state().value();

        switch (state)
        {
            case DISCONNECTING_VALUE:
            {
                if (incorrectBeginString)
                {
                    final int sentMsgSeqNum = newSentSeqNum();
                    final long position = proxy.sendIncorrectBeginStringLogout(
                        sentMsgSeqNum, sequenceIndex(), lastMsgSeqNumProcessed);
                    if (position < 0)
                    {
                        return 1;
                    }
                    lastSentMsgSeqNum(sentMsgSeqNum);
                    requestDisconnect(INCORRECT_BEGIN_STRING);
                }
                else
                {
                    requestDisconnect();
                }

                return 1;
            }

            case LOGGING_OUT_VALUE:
            {
                startLogout();

                return 1;
            }

            case LOGGING_OUT_AND_DISCONNECTING_VALUE:
            {
                final long position = sendLogout();

                state(position < 0 ? LOGGING_OUT_AND_DISCONNECTING : DISCONNECTING);

                return 1;
            }

            default:
            {
                int actions = 0;
                final boolean isActive = state == ACTIVE_VALUE;
                if (isActive && time >= nextRequiredHeartbeatTimeInMs)
                {
                    // Drop when back pressured: retried on duty cycle
                    final int sentSeqNum = newSentSeqNum();
                    final long position = proxy.sendHeartbeat(sentSeqNum, sequenceIndex(), lastMsgSeqNumProcessed);
                    lastSentMsgSeqNum(sentSeqNum, position);
                    actions++;
                }

                if (time >= nextRequiredInboundMessageTimeInMs)
                {
                    if (state == AWAITING_LOGOUT_VALUE || awaitingHeartbeat)
                    {
                        // Drop when back pressured: retried on duty cycle
                        requestDisconnect();
                    }
                    else if (isActive)
                    {
                        final int sentSeqNum = newSentSeqNum();
                        if (proxy.sendTestRequest(
                            sentSeqNum, TEST_REQ_ID, sequenceIndex(), lastMsgSeqNumProcessed) >= 0)
                        {
                            lastSentMsgSeqNum(sentSeqNum);
                            awaitingHeartbeat = true;
                            incNextReceivedInboundMessageTime(time);
                        }
                    }
                    actions++;
                }

                return actions;
            }
        }
    }

    void libraryConnected(final boolean libraryConnected)
    {
        proxy.libraryConnected(libraryConnected);
    }

    void sequenceIndex(final int sequenceIndex)
    {
        this.sequenceIndex = sequenceIndex;
    }

    protected long sendingTime(final long sendingTime, final long origSendingTime)
    {
        return UNKNOWN == origSendingTime ? sendingTime : origSendingTime;
    }

    void logonListener(final SessionLogonListener logonListener)
    {
        this.logonListener = logonListener;
    }

    void logoutRejectReason(final int logoutRejectReason)
    {
        this.logoutRejectReason = logoutRejectReason;
    }

    void address(final String connectedHost, final int connectedPort)
    {
        this.connectedHost = connectedHost;
        this.connectedPort = connectedPort;
    }

    void username(final String username)
    {
        this.username = username;
    }

    void password(final String password)
    {
        this.password = password;
    }

    void logonTime(final long logonTime)
    {
        this.logonTime = logonTime;
    }

    void awaitingResend(final boolean awaitingResend)
    {
        this.awaitingResend = awaitingResend;
    }

    void resendRequestChunkSize(final int resendRequestChunkSize)
    {
        this.resendRequestChunkSize = resendRequestChunkSize;
    }

    void closedResendInterval(final boolean closedResendInterval)
    {
        this.closedResendInterval = closedResendInterval;
    }

    void sendRedundantResendRequests(final boolean sendRedundantResendRequests)
    {
        this.sendRedundantResendRequests = sendRedundantResendRequests;
    }

    void updateLastMessageProcessed()
    {
        if (enableLastMsgSeqNumProcessed)
        {
            lastMsgSeqNumProcessed = lastReceivedMsgSeqNum;
        }
    }

    void initialLastReceivedMsgSeqNum(final int lastReceivedMsgSeqNum)
    {
        lastReceivedMsgSeqNum(lastReceivedMsgSeqNum);
        updateLastMessageProcessed();
    }

    int lastMsgSeqNumProcessed()
    {
        return lastMsgSeqNumProcessed;
    }

    void lastResentMsgSeqNo(final int lastResentMsgSeqNo)
    {
        this.lastResentMsgSeqNo = lastResentMsgSeqNo;
    }

    int lastResentMsgSeqNo()
    {
        return lastResentMsgSeqNo;
    }

    void lastResendChunkMsgSeqNum(final int lastResendChunkMsgSeqNum)
    {
        this.lastResendChunkMsgSeqNum = lastResendChunkMsgSeqNum;
    }

    int lastResendChunkMsgSeqNum()
    {
        return lastResendChunkMsgSeqNum;
    }

    void endOfResendRequestRange(final int endOfResendRequestRange)
    {
        this.endOfResendRequestRange = endOfResendRequestRange;
    }

    int endOfResendRequestRange()
    {
        return endOfResendRequestRange;
    }

    void awaitingHeartbeat(final boolean awaitingHeartbeat)
    {
        this.awaitingHeartbeat = awaitingHeartbeat;
    }

}
