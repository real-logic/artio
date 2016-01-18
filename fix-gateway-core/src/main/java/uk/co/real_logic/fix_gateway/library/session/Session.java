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
package uk.co.real_logic.fix_gateway.library.session;

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.Verify;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.EpochClock;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.SessionRejectReason;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.builder.MessageEncoder;
import uk.co.real_logic.fix_gateway.decoder.*;
import uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.streams.GatewayPublication;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static java.util.concurrent.TimeUnit.SECONDS;
import static uk.co.real_logic.fix_gateway.SessionRejectReason.*;
import static uk.co.real_logic.fix_gateway.builder.Validation.CODEC_VALIDATION_ENABLED;
import static uk.co.real_logic.fix_gateway.decoder.Constants.NEW_SEQ_NO;
import static uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.fix_gateway.library.session.SessionState.*;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.OK;

/**
 * Stores information about the current state of a session - no matter whether outbound or inbound.
 *
 * Should only be accessed on a single thread.
 */
public class Session
{
    public static final long UNKNOWN = -1;
    public static final long NO_OPERATION = -4;

    /**
     * The proportion of the maximum heartbeat interval before you send your heartbeat
     */
    public static final double HEARTBEAT_PAUSE_FACTOR = 0.8;

    public static final String TEST_REQ_ID = "TEST";
    public static final char[] TEST_REQ_ID_CHARS = TEST_REQ_ID.toCharArray();

    private final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();

    private final EpochClock clock;

    protected final SessionProxy proxy;
    protected final long connectionId;
    protected final SessionIdStrategy sessionIdStrategy;
    protected final GatewayPublication publication;
    protected final MutableDirectBuffer buffer;
    protected final MutableAsciiFlyweight string;
    private final char[] expectedBeginString;
    private final long sendingTimeWindow;
    private final AtomicCounter receivedMsgSeqNo;
    private final AtomicCounter sentMsgSeqNo;
    protected final int libraryId;
    protected Object sessionKey;

    private SessionState state;
    private long id = UNKNOWN;
    private int lastReceivedMsgSeqNum = 0;
    private int lastSentMsgSeqNum;

    private long heartbeatIntervalInMs;
    private long nextRequiredInboundMessageTimeInMs;
    private long sendingHeartbeatIntervalInMs;
    private long nextRequiredHeartbeatTimeInMs;

    private String username;
    private String password;
    private String connectedHost;
    private int connectedPort;

    protected Session(
        final int heartbeatIntervalInS,
        final long connectionId,
        final EpochClock clock,
        final SessionState state,
        final SessionProxy proxy,
        final GatewayPublication publication,
        final SessionIdStrategy sessionIdStrategy,
        final char[] expectedBeginString,
        final long sendingTimeWindow,
        final AtomicCounter receivedMsgSeqNo,
        final AtomicCounter sentMsgSeqNo,
        final int libraryId,
        final int sessionBufferSize,
        final int initialSequenceNumber)
    {
        Verify.notNull(clock, "clock");
        Verify.notNull(state, "session state");
        Verify.notNull(proxy, "session proxy");
        Verify.notNull(publication, "publication");
        Verify.notNull(expectedBeginString, "expected begin string");
        Verify.notNull(receivedMsgSeqNo, "received MsgSeqNo counter");
        Verify.notNull(sentMsgSeqNo, "sent MsgSeqNo counter");

        this.clock = clock;
        this.proxy = proxy;
        this.connectionId = connectionId;
        this.publication = publication;
        this.sessionIdStrategy = sessionIdStrategy;
        this.expectedBeginString = expectedBeginString;
        this.sendingTimeWindow = sendingTimeWindow;
        this.receivedMsgSeqNo = receivedMsgSeqNo;
        this.sentMsgSeqNo = sentMsgSeqNo;
        this.libraryId = libraryId;
        lastSentMsgSeqNum = initialSequenceNumber - 1;

        buffer = new UnsafeBuffer(new byte[sessionBufferSize]);
        string = new MutableAsciiFlyweight(buffer);

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
     * @see Session#connectedPort()
     * @return the address of the remote host that your session is connected to.
     */
    public String connectedHost()
    {
        return connectedHost;
    }

    /**
     * Get the id of the connection associated with this session. Sessions always
     * have a connection id.
     *
     * @see Session#id()
     * @return the id of the connection associated with this session.
     */
    public long connectionId()
    {
        return connectionId;
    }

    /**
     * Get the id of this session. If the session hasn't logged in yet, this
     * will return <code>Session.UNKNOWN</code>.
     *
     * @see Session#UNKNOWN
     * @return the id of the session if known.
     */
    public long id()
    {
        return id;
    }

    /**
     * Get the port of the remote host that your session is connected to.
     *
     * @see Session#connectedHost()
     * @return the port of the remote host that your session is connected to.
     */
    public int connectedPort()
    {
        return connectedPort;
    }

    /**
     * Sends a logout message and puts the session into the awaiting logout state.
     *
     * @see Session#logoutAndDisconnect()
     */
    public void startLogout()
    {
        sendLogout();
        awaitLogout();
    }

    /**
     * Request the session be disconnected.
     *
     * @see Session#logoutAndDisconnect()
     */
    public long requestDisconnect()
    {
        long position = NO_OPERATION;
        if (state() != DISCONNECTED)
        {
            position = proxy.requestDisconnect(connectionId);
            state(DISCONNECTED);
        }
        return position;
    }

    /**
     * Send a logout message and immediately disconnect the session.
     *
     * This disconnects the session faster than <code>startLogout</code>.
     *
     * @see Session#startLogout()
     */
    public long logoutAndDisconnect()
    {
        long position = NO_OPERATION;
        if (state() != DISCONNECTED)
        {
            sendLogout();
            position = requestDisconnect();
        }
        return position;
    }

    /**
     * Send a message on this session.
     *
     * @param encoder the encoder of the message to be sent
     * @return the position in the stream that corresponds to the end of this message.
     */
    public long send(final MessageEncoder encoder)
    {
        if (!canSendMessage())
        {
            throw new IllegalStateException("Session isn't active, and thus can't send a message");
        }

        timestampEncoder.encode(time());
        final HeaderEncoder header = (HeaderEncoder) encoder.header();
        header
            .msgSeqNum(newSentSeqNum())
            .sendingTime(timestampEncoder.buffer());

        if (!header.hasSenderCompID())
        {
            sessionIdStrategy.setupSession(sessionKey, header);
        }

        final int length = encoder.encode(string, 0);
        return publication.saveMessage(
            buffer, 0, length, libraryId, encoder.messageType(), id(), connectionId, OK);
    }

    /**
     * Check if the session is in a state where it can send a message.
     *
     * @return true if the session is in a state where it can send a message, false otherwise.
     */
    public boolean canSendMessage()
    {
        return state() == ACTIVE;
    }

    /**
     * Reset the sequence number, so that the specified sequence number will be the sequence
     * number of the next message.
     *
     * @param nextSentMessageSequenceNumber the new sequence number of the next message to be
     *                                      sent.
     * @return the position in the stream that corresponds to the end of this message.
     */
    public long sequenceReset(final int nextSentMessageSequenceNumber)
    {
        final long position = proxy.sequenceReset(lastSentMsgSeqNum, nextSentMessageSequenceNumber);
        lastSentMsgSeqNum = nextSentMessageSequenceNumber - 1;
        return position;
    }

    /**
     * Runs a single iteration of the session's main logic loop. Users of the API don't need to call this method.
     *
     * @see uk.co.real_logic.fix_gateway.library.FixLibrary#poll(int)
     *
     * @param time the current time in milliseconds
     * @return the number of actions performed.
     */
    public int poll(final long time)
    {
        int actions = 0;

        if (time >= nextRequiredHeartbeatTimeInMs)
        {
            proxy.heartbeat(newSentSeqNum());
            actions++;
        }

        if (time >= nextRequiredInboundMessageTimeInMs)
        {
            if (state == AWAITING_LOGOUT || state == AWAITING_RESEND)
            {
                requestDisconnect();
            }
            else
            {
                proxy.testRequest(newSentSeqNum(), TEST_REQ_ID);
                state(AWAITING_RESEND);
                incNextReceivedInboundMessageTime(time);
            }
            actions++;
        }

        return actions;
    }

    // ---------- Event Handlers & Logic ----------

    protected void onDisconnect()
    {
        state(DISCONNECTED);
    }


    void onMessage(final int msgSeqNo,
                   final byte[] msgType,
                   final long sendingTime,
                   final long origSendingTime,
                   final boolean isPossDupOrResend)
    {
        onMessage(msgSeqNo, msgType, msgType.length, sendingTime, origSendingTime, isPossDupOrResend);
    }

    void onMessage(final int msgSeqNo,
                   final byte[] msgType,
                   final int msgTypeLength,
                   final long sendingTime,
                   final long origSendingTime,
                   final boolean isPossDupOrResend)
    {
        if (state() == SessionState.CONNECTED)
        {
            // Disconnect if the first message isn't a logon message
            requestDisconnect();
        }
        else
        {
            if (msgSeqNo == MISSING_INT)
            {
                proxy.receivedMessageWithoutSequenceNumber(newSentSeqNum());
                requestDisconnect();
                return;
            }

            final long time = time();

            if (CODEC_VALIDATION_ENABLED)
            {
                if (isPossDupOrResend)
                {
                    if (origSendingTime == UNKNOWN)
                    {
                        proxy.reject(
                            newSentSeqNum(),
                            msgSeqNo,
                            msgType,
                            msgTypeLength,
                            REQUIRED_TAG_MISSING);
                        return;
                    }
                    else if (origSendingTime > sendingTime)
                    {
                        rejectDueToSendingTime(msgSeqNo, msgType, msgTypeLength);
                        return;
                    }
                }

                if ((sendingTime < time - sendingTimeWindow) || (sendingTime > time + sendingTimeWindow))
                {
                    rejectDueToSendingTime(msgSeqNo, msgType, msgTypeLength);
                    logoutAndDisconnect();
                    return;
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
                proxy.resendRequest(newSentSeqNum(), expectedSeqNo, 0);
            }
            else if (expectedSeqNo > msgSeqNo && !isPossDupOrResend)
            {
                proxy.lowSequenceNumberLogout(newSentSeqNum(), expectedSeqNo, msgSeqNo);
                requestDisconnect();
            }
        }
    }

    private void rejectDueToSendingTime(final int msgSeqNo, final byte[] msgType, final int msgTypeLength)
    {
        proxy.reject(
            newSentSeqNum(),
            msgSeqNo,
            msgType,
            msgTypeLength,
            SENDINGTIME_ACCURACY_PROBLEM);
    }

    private void incNextReceivedInboundMessageTime(final long time)
    {
        nextRequiredMessageTime(time + sendingTimeWindow + heartbeatIntervalInMs());
    }

    void onLogon(final int heartbeatInterval,
                 final int msgSeqNo,
                 final long sessionId,
                 final Object sessionKey,
                 long sendingTime,
                 final long origSendingTime,
                 final String username,
                 final String password,
                 final boolean isPossDupOrResend)
    {
        throw new UnsupportedOperationException();
    }

    protected boolean validateSendingTime(final long sendingTime)
    {
        final long time = time();
        final boolean isValid = sendingTime < (time + sendingTimeWindow) && sendingTime > (time - sendingTimeWindow);

        if (!isValid)
        {
            proxy.rejectWhilstNotLoggedOn(newSentSeqNum(), SENDINGTIME_ACCURACY_PROBLEM);
            requestDisconnect();
        }

        return isValid;
    }

    protected boolean validateHeartbeat(int heartbeatInterval)
    {
        if (heartbeatInterval < 0)
        {
            proxy.negativeHeartbeatLogout(newSentSeqNum());
            requestDisconnect();
            return false;
        }
        else
        {
            return true;
        }
    }

    void onLogout(
        final int msgSeqNo,
        final long sendingTime,
        final long origSendingTime,
        final boolean isPossDupOrResend)
    {
        onMessage(msgSeqNo, LogoutDecoder.MESSAGE_TYPE_BYTES, sendingTime, origSendingTime, isPossDupOrResend);
        if (state() == AWAITING_LOGOUT)
        {
            requestDisconnect();
        }
        else
        {
            logoutAndDisconnect();
        }
    }

    void onTestRequest(
        final int msgSeqNo,
        final char[] testReqId,
        final int testReqIdLength,
        final long sendingTime,
        final long origSendingTime,
        final boolean isPossDupOrResend)
    {
        if (msgSeqNo != MISSING_INT)
        {
            proxy.heartbeat(testReqId, testReqIdLength, newSentSeqNum());
        }
        onMessage(msgSeqNo, TestRequestDecoder.MESSAGE_TYPE_BYTES, sendingTime, origSendingTime, isPossDupOrResend);
    }

    void onSequenceReset(final int msgSeqNo, final int newSeqNo, final boolean gapFillFlag, final boolean possDupFlag)
    {
        if (!gapFillFlag)
        {
            applySequenceReset(msgSeqNo, newSeqNo);
        }
        else if (newSeqNo > msgSeqNo)
        {
            gapFill(msgSeqNo, newSeqNo, possDupFlag);
        }
        else
        {
            applySequenceReset(msgSeqNo, newSeqNo);
        }
    }

    private void applySequenceReset(final int receivedMsgSeqNo, final int newSeqNo)
    {
        final int expectedMsgSeqNo = expectedReceivedSeqNum();
        if (newSeqNo > expectedMsgSeqNo)
        {
            lastReceivedMsgSeqNum(newSeqNo - 1);
        }
        else if (newSeqNo < expectedMsgSeqNo)
        {
            proxy.reject(
                newSentSeqNum(),
                receivedMsgSeqNo,
                NEW_SEQ_NO,
                SequenceResetDecoder.MESSAGE_TYPE_BYTES,
                SequenceResetDecoder.MESSAGE_TYPE_BYTES.length,
                SessionRejectReason.VALUE_IS_INCORRECT);
        }
    }

    private void gapFill(final int receivedMsgSeqNo, final int newSeqNo, final boolean possDupFlag)
    {
        final int expectedMsgSeqNo = expectedReceivedSeqNum();
        if (receivedMsgSeqNo > expectedMsgSeqNo)
        {
            proxy.resendRequest(newSentSeqNum(), expectedMsgSeqNo, 0);
            lastReceivedMsgSeqNum(newSeqNo - 1);
        }
        else if (receivedMsgSeqNo < expectedMsgSeqNo)
        {
            if (!possDupFlag)
            {
                proxy.lowSequenceNumberLogout(newSentSeqNum(), expectedMsgSeqNo, receivedMsgSeqNo);
                requestDisconnect();
            }
        }
        else
        {
            lastReceivedMsgSeqNum(newSeqNo - 1);
        }
    }

    void onReject(final int msgSeqNo,
                  final long sendingTime,
                  final long origSendingTime,
                  final boolean isPossDupOrResend)
    {
        onMessage(msgSeqNo, RejectDecoder.MESSAGE_TYPE_BYTES, sendingTime, origSendingTime, isPossDupOrResend);
    }

    public boolean onBeginString(final char[] value, final int length, final boolean isLogon)
    {
        final boolean isValid = CodecUtil.equals(value, expectedBeginString, length);
        if (!isValid)
        {
            if (!isLogon)
            {
                proxy.incorrectBeginStringLogout(newSentSeqNum());
            }
            requestDisconnect();
        }
        return isValid;
    }

    private void incNextHeartbeatTime()
    {
        nextRequiredHeartbeatTimeInMs = time() + sendingHeartbeatIntervalInMs;
    }

    private void awaitLogout()
    {
        state(AWAITING_LOGOUT);
    }

    private void sendLogout()
    {
        proxy.logout(newSentSeqNum());
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

    Session nextRequiredMessageTime(final long nextRequiredMessageTime)
    {
        this.nextRequiredInboundMessageTimeInMs = nextRequiredMessageTime;
        return this;
    }

    protected Session state(final SessionState state)
    {
        this.state = state;
        return this;
    }

    protected Session id(final long id)
    {
        this.id = id;
        return this;
    }

    protected long time()
    {
        return clock.time();
    }

    protected Session lastReceivedMsgSeqNum(final int value)
    {
        this.lastReceivedMsgSeqNum = value;
        receivedMsgSeqNo.setOrdered(value);
        return this;
    }

    protected int expectedReceivedSeqNum()
    {
        return lastReceivedMsgSeqNum + 1;
    }

    protected int newSentSeqNum()
    {
        final int lastSentMsgSeqNum = ++this.lastSentMsgSeqNum;
        sentMsgSeqNo.setOrdered(lastSentMsgSeqNum);
        incNextHeartbeatTime();
        return lastSentMsgSeqNum;
    }

    protected void incReceivedSeqNum()
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

    protected void username(final String username)
    {
        this.username = username;
    }

    protected void password(final String password)
    {
        this.password = password;
    }

    public void onInvalidMessage(
        final int refSeqNum,
        final int refTagId,
        final char[] refMsgType,
        final int refMsgTypeLength,
        final int rejectReason)
    {
        incReceivedSeqNum();

        proxy.reject(
            newSentSeqNum(),
            refSeqNum,
            refTagId,
            refMsgType,
            refMsgTypeLength,
            rejectReason);
    }

    protected void onHeartbeat(
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
        onMessage(msgSeqNum, HeartbeatDecoder.MESSAGE_TYPE_BYTES, sendingTime, origSendingTime, isPossDupOrResend);
    }

    protected void onInvalidMessageType(final int msgSeqNum, final char[] msgType, final int msgTypeLength)
    {
        proxy.reject(
            newSentSeqNum(),
            msgSeqNum,
            msgType,
            msgTypeLength,
            INVALID_MSGTYPE.representation());
    }

    public String toString()
    {
        return getClass().getSimpleName() + "{" +
            "connectionId=" + connectionId +
            ", sessionId=" + id +
            ", state=" + state +
            '}';
    }
}
