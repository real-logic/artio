/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.session;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.Verify;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.CommonConfiguration;
import uk.co.real_logic.fix_gateway.Pressure;
import uk.co.real_logic.fix_gateway.builder.Encoder;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.decoder.*;
import uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil;
import uk.co.real_logic.fix_gateway.fields.RejectReason;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.messages.SessionState;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.lang.Integer.MIN_VALUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static uk.co.real_logic.fix_gateway.builder.Validation.CODEC_VALIDATION_DISABLED;
import static uk.co.real_logic.fix_gateway.builder.Validation.CODEC_VALIDATION_ENABLED;
import static uk.co.real_logic.fix_gateway.decoder.Constants.NEW_SEQ_NO;
import static uk.co.real_logic.fix_gateway.decoder.Constants.VERSION_CHARS;
import static uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil.MISSING_LONG;
import static uk.co.real_logic.fix_gateway.fields.RejectReason.*;
import static uk.co.real_logic.fix_gateway.messages.DisconnectReason.*;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.OK;
import static uk.co.real_logic.fix_gateway.messages.SessionState.*;

/**
 * Stores information about the current state of a session - no matter whether outbound or inbound.
 * <p>
 * Should only be accessed on a single thread.
 * <p>
 * <h1>State Transitions</h1>
 * <p>
 * Successful Login: CONNECTED -> ACTIVE
 * Login with high sequence number: CONNECTED -> AWAITING_RESEND
 * Login with low sequence number: CONNECTED -> DISCONNECTED
 * Login with wrong credentials: CONNECTED -> DISCONNECTED or CONNECTED -> DISABLED
 * depending on authentication plugin
 * <p>
 * Successful Hijack: * -> ACTIVE (same as regular login)
 * Hijack with high sequence number: * -> AWAITING_RESEND (same as regular login)
 * Hijack with low sequence number: requestDisconnect the hijacker and leave main system ACTIVE
 * Hijack with wrong credentials: requestDisconnect the hijacker and leave main system ACTIVE
 * <p>
 * Successful resend: AWAITING_RESEND -> ACTIVE
 * <p>
 * Send test request: ACTIVE -> ACTIVE - but alter the timeout for the next expected heartbeat.
 * Successful Heartbeat: ACTIVE -> ACTIVE - updates the timeout time.
 * Heartbeat Timeout: ACTIVE -> DISCONNECTED
 * <p>
 * Logout request: ACTIVE -> AWAITING_LOGOUT
 * Logout acknowledgement: AWAITING_LOGOUT -> DISCONNECTED
 * <p>
 * Manual disable: * -> DISABLED
 */
public class Session implements AutoCloseable
{
    public static final long UNKNOWN = -1;
    public static final long NO_OPERATION = MIN_VALUE;
    public static final long LIBRARY_DISCONNECTED = NO_OPERATION + 1;
    public static final long NO_LOGON_TIME = -1;
    public static final long INITIAL_SEQUENCE_NUMBER = 1;

    static final short ACTIVE_VALUE = 3;
    static final short AWAITING_RESEND_VALUE = 4;
    static final short LOGGING_OUT_VALUE = 5;
    static final short LOGGING_OUT_AND_DISCONNECTING_VALUE = 6;
    static final short AWAITING_LOGOUT_VALUE = 7;
    static final short DISCONNECTING_VALUE = 8;

    /**
     * The proportion of the maximum heartbeat interval before you send your heartbeat
     */
    public static final double HEARTBEAT_PAUSE_FACTOR = 0.8;

    static final String TEST_REQ_ID = "TEST";
    private static final char[] TEST_REQ_ID_CHARS = TEST_REQ_ID.toCharArray();

    private final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();

    protected final long connectionId;
    protected final SessionIdStrategy sessionIdStrategy;
    protected final GatewayPublication publication;
    protected final MutableAsciiBuffer asciiBuffer;
    protected final int libraryId;

    final SessionProxy proxy;

    private final EpochClock clock;
    private final long sendingTimeWindowInMs;
    private final AtomicCounter receivedMsgSeqNo;
    private final AtomicCounter sentMsgSeqNo;
    private final long reasonableTransmissionTimeInMs;

    CompositeKey sessionKey;

    private SessionState state;
    private long id = UNKNOWN;
    private int lastReceivedMsgSeqNum = 0;
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

    private boolean incorrectBeginString = false;
    protected boolean resendSaveLogon = false;

    public Session(
        final int heartbeatIntervalInS,
        final long connectionId,
        final EpochClock clock,
        final SessionState state,
        final SessionProxy proxy,
        final GatewayPublication publication,
        final SessionIdStrategy sessionIdStrategy,
        final long sendingTimeWindowInMs,
        final AtomicCounter receivedMsgSeqNo,
        final AtomicCounter sentMsgSeqNo,
        final int libraryId,
        final int initialSequenceNumber,
        final int sequenceIndex,
        final long reasonableTransmissionTimeInMs,
        final MutableAsciiBuffer asciiBuffer)
    {
        Verify.notNull(clock, "clock");
        Verify.notNull(state, "session state");
        Verify.notNull(proxy, "session proxy");
        Verify.notNull(publication, "publication");
        Verify.notNull(receivedMsgSeqNo, "received MsgSeqNo counter");
        Verify.notNull(sentMsgSeqNo, "sent MsgSeqNo counter");

        this.clock = clock;
        this.proxy = proxy;
        this.connectionId = connectionId;
        this.publication = publication;
        this.sessionIdStrategy = sessionIdStrategy;
        this.sendingTimeWindowInMs = sendingTimeWindowInMs;
        this.receivedMsgSeqNo = receivedMsgSeqNo;
        this.sentMsgSeqNo = sentMsgSeqNo;
        this.libraryId = libraryId;
        sequenceIndex(sequenceIndex);
        this.lastSentMsgSeqNum = initialSequenceNumber - 1;
        this.reasonableTransmissionTimeInMs = reasonableTransmissionTimeInMs;

        this.asciiBuffer = asciiBuffer;

        state(state);
        heartbeatIntervalInS(heartbeatIntervalInS);
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
        return state != CONNECTING
               && state != DISCONNECTED
               && state != DISABLED;
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
            position = proxy.requestDisconnect(connectionId, reason);
            state(position < 0 ? DISCONNECTING : DISCONNECTED);
        }

        return position;
    }

    /**
     * Send a logout message and immediately disconnect the session.
     * <p>
     * This disconnects the session faster than <code>startLogout</code>.
     *
     * @see Session#startLogout()
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
        if (!canSendMessage())
        {
            throw new IllegalStateException(
                String.format("Session isn't active it's %s, and thus can't send a message", state));
        }

        final int sentSeqNum = newSentSeqNum();
        final HeaderEncoder header = (HeaderEncoder) encoder.header();
        header
            .msgSeqNum(sentSeqNum)
            .sendingTime(timestampEncoder.buffer(), timestampEncoder.encode(time()));

        if (!header.hasSenderCompID())
        {
            sessionIdStrategy.setupSession(sessionKey, header);
        }

        final long result = encoder.encode(asciiBuffer, 0);
        final int length = Encoder.length(result);
        final int offset = Encoder.offset(result);
        final long position = publication.saveMessage(
            asciiBuffer, offset, length, libraryId, encoder.messageType(), id(), sequenceIndex(), connectionId, OK);
        lastSentMsgSeqNum(sentSeqNum, position);

        return position;
    }

    private void lastSentMsgSeqNum(final int sentSeqNum, final long position)
    {
        if (position >= 0)
        {
            lastSentMsgSeqNum(sentSeqNum);
        }
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
    public long sendSequenceReset(final int nextSentMessageSequenceNumber)
    {
        nextSequenceIndex();
        final long position = proxy.sequenceReset(lastSentMsgSeqNum, nextSentMessageSequenceNumber, sequenceIndex());
        lastSentMsgSeqNum(nextSentMessageSequenceNumber - 1, position);

        return position;
    }

    protected void nextSequenceIndex()
    {
        sequenceIndex++;
    }

    /**
     * Resets both the receiver and sender sequence numbers of this session. This is equivalent to
     * sending a Logon message with ResetSeqNum flag set to Y.
     * <p>
     * If you want to send a sequence reset message then you should use {@link #sendSequenceReset(int)}.
     *
     * @return the position in the stream that corresponds to the end of this message.
     */
    public long resetSequenceNumbers()
    {
        final int sentSeqNum = 1;
        final int heartbeatIntervalInS = (int) MILLISECONDS.toSeconds(heartbeatIntervalInMs);
        nextSequenceIndex();
        final long position = proxy.logon(
            heartbeatIntervalInS, sentSeqNum, username(), password(), true, sequenceIndex());
        lastSentMsgSeqNum(sentSeqNum, position);

        return position;
    }

    /**
     * Runs a single iteration of the session's main logic loop. Users of the API don't need to call this method.
     *
     * @param time the current time in milliseconds
     * @return the number of actions performed.
     * @see uk.co.real_logic.fix_gateway.library.FixLibrary#poll(int)
     */
    public int poll(final long time)
    {
        final short state = state().value();

        switch (state)
        {
            case DISCONNECTING_VALUE:
            {
                if (incorrectBeginString)
                {
                    final int sentMsgSeqNum = newSentSeqNum();
                    final long position = proxy.incorrectBeginStringLogout(sentMsgSeqNum, sequenceIndex());
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
                final boolean isActive = state == ACTIVE_VALUE || state == AWAITING_RESEND_VALUE;
                if (isActive && time >= nextRequiredHeartbeatTimeInMs)
                {
                    // Drop when back pressured: retried on duty cycle
                    final int sentSeqNum = newSentSeqNum();
                    final long position = proxy.heartbeat(sentSeqNum, sequenceIndex());
                    lastSentMsgSeqNum(sentSeqNum, position);
                    actions++;
                }

                if (time >= nextRequiredInboundMessageTimeInMs)
                {
                    if (state == AWAITING_LOGOUT_VALUE || state == AWAITING_RESEND_VALUE)
                    {
                        // Drop when back pressured: retried on duty cycle
                        requestDisconnect();
                    }
                    else if (isActive)
                    {
                        final int sentSeqNum = newSentSeqNum();
                        if (proxy.testRequest(sentSeqNum, TEST_REQ_ID, sequenceIndex()) >= 0)
                        {
                            lastSentMsgSeqNum(sentSeqNum);
                            state(AWAITING_RESEND);
                            incNextReceivedInboundMessageTime(time);
                        }
                    }
                    actions++;
                }

                return actions;
            }
        }
    }

    public boolean isActive()
    {
        final SessionState state = this.state;
        return state == ACTIVE || state == AWAITING_RESEND;
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

    Action onRequestDisconnect(final DisconnectReason reason)
    {
        return Pressure.apply(requestDisconnect(reason));
    }

    public void onDisconnect()
    {
        state(DISCONNECTED);
    }

    Action onMessage(
        final int msgSeqNo,
        final byte[] msgType,
        final long sendingTime,
        final long origSendingTime,
        final boolean isPossDupOrResend)
    {
        return onMessage(msgSeqNo, msgType, msgType.length, sendingTime, origSendingTime, isPossDupOrResend);
    }

    Action onMessage(
        final int msgSeqNo,
        final byte[] msgType,
        final int msgTypeLength,
        final long sendingTime,
        final long origSendingTime,
        final boolean isPossDupOrResend)
    {
        if (state() == SessionState.CONNECTED)
        {
            // Disconnect if the first message isn't a logon message
            return Pressure.apply(requestDisconnect(FIRST_MESSAGE_NOT_LOGON));
        }
        else
        {
            if (msgSeqNo == MISSING_INT)
            {
                final int sentSeqNum = newSentSeqNum();
                return checkPositionAndDisconnect(
                    proxy.receivedMessageWithoutSequenceNumber(sentSeqNum, sequenceIndex()),
                    MSG_SEQ_NO_MISSING);
            }

            final long time = time();

            if (CODEC_VALIDATION_ENABLED)
            {
                if (isPossDupOrResend)
                {
                    if (origSendingTime == UNKNOWN)
                    {
                        return checkPosition(proxy.reject(
                            newSentSeqNum(),
                            msgSeqNo,
                            msgType,
                            msgTypeLength,
                            REQUIRED_TAG_MISSING, sequenceIndex()));
                    }
                    else if (origSendingTime > sendingTime)
                    {
                        return rejectDueToSendingTime(msgSeqNo, msgType, msgTypeLength);
                    }
                }

                if ((sendingTime < time - sendingTimeWindowInMs) || (sendingTime > time + sendingTimeWindowInMs))
                {
                    final Action action = rejectDueToSendingTime(msgSeqNo, msgType, msgTypeLength);
                    if (action != ABORT)
                    {
                        logoutAndDisconnect(INVALID_SENDING_TIME);
                    }

                    return action;
                }
            }

            final int expectedSeqNo = expectedReceivedSeqNum();
            if (expectedSeqNo == msgSeqNo)
            {
                incNextReceivedInboundMessageTime(time);
                lastReceivedMsgSeqNum(msgSeqNo);
            }
            else if (expectedSeqNo < msgSeqNo)
            {
                state(AWAITING_RESEND);
                return checkPosition(
                    proxy.resendRequest(newSentSeqNum(), expectedSeqNo, 0, sequenceIndex()));
            }
            else if (expectedSeqNo > msgSeqNo && !isPossDupOrResend)
            {
                return checkPositionAndDisconnect(
                    proxy.lowSequenceNumberLogout(newSentSeqNum(), expectedSeqNo, msgSeqNo, sequenceIndex()),
                    MSG_SEQ_NO_TOO_LOW);
            }
        }

        return CONTINUE;
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

    private Action rejectDueToSendingTime(final int msgSeqNo, final byte[] msgType, final int msgTypeLength)
    {
        return checkPosition(proxy.reject(
            newSentSeqNum(),
            msgSeqNo,
            msgType,
            msgTypeLength,
            SENDINGTIME_ACCURACY_PROBLEM, sequenceIndex()));
    }

    private void incNextReceivedInboundMessageTime(final long time)
    {
        this.nextRequiredInboundMessageTimeInMs = time + heartbeatIntervalInMs() + reasonableTransmissionTimeInMs;
    }

    public Action onLogon(
        final int heartbeatInterval,
        final int msgSeqNo,
        final long sessionId,
        final CompositeKey sessionKey,
        final long sendingTime,
        final long origSendingTime,
        final String username,
        final String password,
        final boolean isPossDupOrResend,
        final boolean resetSeqNumFlag)
    {
        setupSession(sessionId, sessionKey);

        final long logonTime = sendingTime(sendingTime, origSendingTime);

        if (state() == SessionState.CONNECTED)
        {
            resendSaveLogon = true;
            Action action = validateHeartbeat(heartbeatInterval);
            if (action != null)
            {
                return action;
            }

            action = validateSendingTime(sendingTime);
            if (action != null)
            {
                return action;
            }

            final int expectedSeqNo = expectedReceivedSeqNum();
            if (expectedSeqNo == msgSeqNo)
            {
                action = replyToLogon(heartbeatInterval, resetSeqNumFlag);
                if (action == ABORT)
                {
                    return ABORT;
                }

                setLogonState(heartbeatInterval, username, password);

                if (INITIAL_SEQUENCE_NUMBER == msgSeqNo)
                {
                    // Incoming initiators are allowed to start a session from 1
                    // Without also sending 141=Y if the session was previously logged out cleanly.
                    logonTime(logonTime);
                }
            }
            else if (expectedSeqNo < msgSeqNo)
            {
                action = replyToLogon(heartbeatInterval, resetSeqNumFlag);
                if (action == ABORT)
                {
                    return ABORT;
                }

                state(SessionState.AWAITING_RESEND);
            }
        }

        if (resetSeqNumFlag)
        {
            final Action action = onResetSeqNumLogon(heartbeatInterval, msgSeqNo, username, password, logonTime);
            if (action != null)
            {
                return action;
            }

        }

        if (checkResendSaveLogon(sessionId))
        {
            return ABORT;
        }

        // Back pressure at this point won't re-run the above block if its completed because of the state change
        return onMessage(msgSeqNo, LogonDecoder.MESSAGE_TYPE_BYTES, sendingTime, origSendingTime, isPossDupOrResend);
    }

    private boolean checkResendSaveLogon(final long sessionId)
    {
        if (resendSaveLogon)
        {
            // TODO(Nick): Why is this published? Do we have enough details?
            final long position = publication.saveManageSession(libraryId, connectionId, sessionId);
            if (position < 0)
            {
                return true;
            }
            resendSaveLogon = false;
        }
        return false;
    }

    Action onResetSeqNumLogon(
        final int heartbeatInterval,
        final int msgSeqNo,
        final String username,
        final String password,
        final long sendingTime)
    {
        if (lastSentMsgSeqNum() == 1)
        {
            // You've received a reply to a resetSeqNumFlag = Y message
            lastReceivedMsgSeqNumOnly(1);
            setLogonState(heartbeatInterval, username, password);
            // logon time becomes time of the confirmation message.
            logonTime(sendingTime);

            // TODO(Nick): Publish event to signify the session start time should be reset.
            return CONTINUE;
        }

        final int newSeqNum = 1;
        final long position = proxy.logon(heartbeatInterval, newSeqNum, null, null, true, sequenceIndex() + 1);
        if (position < 0)
        {
            return ABORT;
        }
        else
        {
            lastSentMsgSeqNum(newSeqNum);
            lastReceivedMsgSeqNum(msgSeqNo - 1); // onMessage will check and increment this
            setLogonState(heartbeatInterval, username, password);
            return null;
        }

    }

    private void setLogonState(final int heartbeatInterval, final String username, final String password)
    {
        heartbeatIntervalInS(heartbeatInterval);
        state(ACTIVE);
        username(username);
        password(password);
    }

    public void setupSession(final long sessionId, final CompositeKey sessionKey)
    {
        id(sessionId);
        this.sessionKey = sessionKey;
        proxy.setupSession(sessionId, sessionKey);
    }

    private Action replyToLogon(final int heartbeatInterval, final boolean resetSeqNumFlag)
    {
        return checkPosition(proxy.logon(
            heartbeatInterval, newSentSeqNum(), null, null, resetSeqNumFlag, sequenceIndex()));
    }

    Action validateSendingTime(final long sendingTime)
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
            proxy.rejectWhilstNotLoggedOn(newSentSeqNum(), SENDINGTIME_ACCURACY_PROBLEM, sequenceIndex()),
            INVALID_SENDING_TIME);
    }

    Action validateHeartbeat(final int heartbeatInterval)
    {
        if (heartbeatInterval < 0)
        {
            return checkPositionAndDisconnect(
                proxy.negativeHeartbeatLogout(newSentSeqNum(), sequenceIndex()),
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

    Action onLogout(
        final int msgSeqNo,
        final long sendingTime,
        final long origSendingTime,
        final boolean isPossDupOrResend)
    {
        final Action action = onMessage(
            msgSeqNo, LogoutDecoder.MESSAGE_TYPE_BYTES, sendingTime, origSendingTime, isPossDupOrResend);
        if (action == ABORT)
        {
            return ABORT;
        }

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
        final boolean isPossDupOrResend)
    {
        if (msgSeqNo != MISSING_INT)
        {
            final int sentSeqNum = newSentSeqNum();
            final long position = proxy.heartbeat(testReqId, testReqIdLength, sentSeqNum, sequenceIndex());
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
            msgSeqNo, TestRequestDecoder.MESSAGE_TYPE_BYTES, sendingTime, origSendingTime, isPossDupOrResend);
    }

    Action onSequenceReset(final int msgSeqNo, final int newSeqNo, final boolean gapFillFlag, final boolean possDupFlag)
    {
        if (!gapFillFlag)
        {
            return applySequenceReset(msgSeqNo, newSeqNo);
        }
        else if (newSeqNo > msgSeqNo)
        {
            return gapFill(msgSeqNo, newSeqNo, possDupFlag);
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
            return checkPosition(proxy.reject(
                newSentSeqNum(),
                receivedMsgSeqNo,
                NEW_SEQ_NO,
                SequenceResetDecoder.MESSAGE_TYPE_BYTES,
                SequenceResetDecoder.MESSAGE_TYPE_BYTES.length,
                RejectReason.VALUE_IS_INCORRECT, sequenceIndex()));
        }

        return CONTINUE;
    }

    private Action gapFill(final int receivedMsgSeqNo, final int newSeqNo, final boolean possDupFlag)
    {
        final int expectedMsgSeqNo = expectedReceivedSeqNum();
        if (receivedMsgSeqNo > expectedMsgSeqNo)
        {
            final Action action = checkPosition(
                proxy.resendRequest(newSentSeqNum(), expectedMsgSeqNo, 0, sequenceIndex()));
            if (action != ABORT)
            {
                lastReceivedMsgSeqNum(newSeqNo - 1);
            }
            return action;
        }
        else if (receivedMsgSeqNo < expectedMsgSeqNo)
        {
            if (!possDupFlag)
            {
                return checkPositionAndDisconnect(
                    proxy.lowSequenceNumberLogout(newSentSeqNum(), expectedMsgSeqNo, receivedMsgSeqNo, sequenceIndex()),
                    MSG_SEQ_NO_TOO_LOW);
            }
        }
        else
        {
            lastReceivedMsgSeqNum(newSeqNo - 1);
        }

        return CONTINUE;
    }

    Action onReject(
        final int msgSeqNo,
        final long sendingTime,
        final long origSendingTime,
        final boolean isPossDupOrResend)
    {
        return onMessage(msgSeqNo, RejectDecoder.MESSAGE_TYPE_BYTES, sendingTime, origSendingTime, isPossDupOrResend);
    }

    boolean onBeginString(final char[] value, final int length, final boolean isLogon)
    {
        final boolean isValid = CodecUtil.equals(value, VERSION_CHARS, length);
        if (!isValid)
        {
            if (!isLogon)
            {
                final int sentMsgSeqNum = newSentSeqNum();
                final long position = proxy.incorrectBeginStringLogout(sentMsgSeqNum, sequenceIndex());
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
        final long position = proxy.logout(sentSeqNum, sequenceIndex());
        if (position >= 0)
        {
            lastSentMsgSeqNum(sentSeqNum);
        }

        return position;
    }

    // ---------- Setters ----------

    Session heartbeatIntervalInS(final int heartbeatIntervalInS)
    {
        this.heartbeatIntervalInMs = SECONDS.toMillis((long) heartbeatIntervalInS);

        final long time = time();
        incNextReceivedInboundMessageTime(time);
        sendingHeartbeatIntervalInMs = (long) (heartbeatIntervalInMs * HEARTBEAT_PAUSE_FACTOR);
        nextRequiredHeartbeatTimeInMs = time + sendingHeartbeatIntervalInMs;

        return this;
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
        return clock.time();
    }

    // Also checks the sequence index
    public Session lastReceivedMsgSeqNum(final int value)
    {
        if (lastReceivedMsgSeqNum > value)
        {
            nextSequenceIndex();
        }

        lastReceivedMsgSeqNumOnly(value);

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

    public Session address(final String connectedHost, final int connectedPort)
    {
        this.connectedHost = connectedHost;
        this.connectedPort = connectedPort;

        return this;
    }

    public Session username(final String username)
    {
        this.username = username;
        return this;
    }

    public Session password(final String password)
    {
        this.password = password;
        return this;
    }

    public Session logonTime(final long logonTime)
    {
        this.logonTime = logonTime;
        return this;
    }

    public long logonTime()
    {
        return this.logonTime;
    }

    // Visible for testing
    public Action onInvalidMessage(
        final int refSeqNum,
        final int refTagId,
        final char[] refMsgType,
        final int refMsgTypeLength,
        final int rejectReason)
    {
        final Action action = checkPosition(proxy.reject(
            newSentSeqNum(),
            refSeqNum,
            refTagId,
            refMsgType,
            refMsgTypeLength,
            rejectReason, sequenceIndex()));

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
        final boolean isPossDupOrResend)
    {
        if (state == AWAITING_RESEND && CodecUtil.equals(testReqID, TEST_REQ_ID_CHARS, testReqIDLength))
        {
            state(ACTIVE);
        }

        return onMessage(
            msgSeqNum, HeartbeatDecoder.MESSAGE_TYPE_BYTES, sendingTime, origSendingTime, isPossDupOrResend);
    }

    Action onInvalidMessageType(final int msgSeqNum, final char[] msgType, final int msgTypeLength)
    {
        return checkPosition(proxy.reject(
            newSentSeqNum(),
            msgSeqNum,
            msgType,
            msgTypeLength,
            INVALID_MSGTYPE.representation(), sequenceIndex()));
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
}
