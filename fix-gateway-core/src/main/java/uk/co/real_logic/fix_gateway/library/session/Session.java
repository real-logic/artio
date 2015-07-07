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
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.builder.MessageEncoder;
import uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.util.MilliClock;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static uk.co.real_logic.fix_gateway.SessionRejectReason.SENDINGTIME_ACCURACY_PROBLEM;
import static uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.fix_gateway.library.session.SessionState.ACTIVE;

/**
 * Stores information about the current state of a session - no matter whether outbound or inbound.
 *
 * Should only be accessed on a single thread.
 */
public class Session
{
    public static final long UNKNOWN_ID = -1;

    /**
     * The proportion of the maximum heartbeat interval before you send your heartbeat
     */
    public static final double HEARTBEAT_PAUSE_FACTOR = 0.8;

    private final MilliClock clock;

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
    protected Object sessionKey;

    private SessionState state;
    private long id = UNKNOWN_ID;
    // TODO: unify this with the atomic counter
    private int lastReceivedMsgSeqNum = 0;
    private int lastSentMsgSeqNum = 0;

    private long heartbeatIntervalInMs;
    private long nextRequiredMessageTimeInMs;
    private long sendingHeartbeatIntervalInMs;
    private long nextRequiredHeartbeatTimeInMs;

    public Session(
        final int heartbeatIntervalInS,
        final long connectionId,
        final MilliClock clock,
        final SessionState state,
        final SessionProxy proxy,
        final GatewayPublication publication,
        final SessionIdStrategy sessionIdStrategy,
        final char[] expectedBeginString,
        final long sendingTimeWindow,
        final AtomicCounter receivedMsgSeqNo,
        final AtomicCounter sentMsgSeqNo)
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

        buffer = new UnsafeBuffer(new byte[8 * 1024]);
        string = new MutableAsciiFlyweight(buffer);

        state(state);

        heartbeatIntervalInS(heartbeatIntervalInS);
    }

    // ---------- PUBLIC API ----------

    public boolean isConnected()
    {
        return state() != SessionState.CONNECTING && state() != SessionState.DISCONNECTED && state() != SessionState.DISABLED;
    }

    public SessionState state()
    {
        return state;
    }

    public int poll(final long time)
    {
        int actions = 0;

        if (time >= nextRequiredMessageTimeInMs)
        {
            if (state() == SessionState.AWAITING_LOGOUT)
            {
                requestDisconnect();
            }
            else
            {
                startLogout();
            }
            actions++;
        }

        if (time >= nextRequiredHeartbeatTimeInMs)
        {
            proxy.heartbeat(newSentSeqNum());
            nextRequiredHeartbeatTimeInMs += sendingHeartbeatIntervalInMs;
            actions++;
        }

        return actions;
    }

    public void startLogout()
    {
        sendLogout();
        awaitLogout();
    }

    private void awaitLogout()
    {
        state(SessionState.AWAITING_LOGOUT);
    }

    private void sendLogout()
    {
        proxy.logout(newSentSeqNum());
    }

    public void requestDisconnect()
    {
        if (state() != SessionState.DISCONNECTED)
        {
            proxy.requestDisconnect(connectionId);
            state(SessionState.DISCONNECTED);
        }
    }

    public long send(final MessageEncoder encoder)
    {
        if (state() != ACTIVE)
        {
            throw new IllegalStateException("Session isn't active, and thus can't send a message");
        }

        final HeaderEncoder header = (HeaderEncoder) encoder.header();
        header
            .msgSeqNum(newSentSeqNum())
            .sendingTime(time());
        // TODO: figure out the best way to remove this overhead from every send
        sessionIdStrategy.setupSession(sessionKey, header);

        final int length = encoder.encode(string, 0);

        return publication.saveMessage(buffer, 0, length, encoder.messageType(), id(), connectionId);
    }

    // ---------- Event Handlers ----------

    void onMessage(final int msgSeqNo, final boolean isPossDupOrResend)
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

            final int expectedSeqNo = expectedReceivedSeqNum();
            if (expectedSeqNo == msgSeqNo)
            {
                nextRequiredMessageTime(time() + heartbeatIntervalInMs());
                lastReceivedMsgSeqNum(msgSeqNo);
            }
            else if (expectedSeqNo < msgSeqNo)
            {
                state(SessionState.AWAITING_RESEND);
                proxy.resendRequest(newSentSeqNum(), expectedSeqNo, msgSeqNo - 1);
                incReceivedSeqNum();
            }
            else if (expectedSeqNo > msgSeqNo && !isPossDupOrResend)
            {
                proxy.lowSequenceNumberLogout(newSentSeqNum(), expectedSeqNo, msgSeqNo);
                requestDisconnect();
            }
        }
    }

    void onLogon(final int heartbeatInterval,
                 final int msgSeqNo,
                 final long sessionId,
                 final Object sessionKey,
                 long sendingTime,
                 final boolean isPossDupOrResend)
    {
        this.sessionKey = sessionKey;
        proxy.setupSession(sessionId, sessionKey);
        if (validateHeartbeat(heartbeatInterval) && validateSendingTime(sendingTime))
        {
            id(sessionId);
            heartbeatIntervalInS(heartbeatInterval);
            onMessage(msgSeqNo, isPossDupOrResend);
            publication.saveLogon(connectionId, sessionId);
        }
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

    void onLogout(final int msgSeqNo, final boolean isPossDupOrResend)
    {
        onMessage(msgSeqNo, isPossDupOrResend);
        if (state() == SessionState.AWAITING_LOGOUT)
        {
            requestDisconnect();
        }
        else
        {
            logoutAndDisconnect();
        }
    }

    public void logoutAndDisconnect()
    {
        sendLogout();
        requestDisconnect();
    }

    public void onDisconnect()
    {
        state(SessionState.DISCONNECTED);
    }

    void onTestRequest(
        final char[] testReqId, final int testReqIdLength, final int msgSeqNo, final boolean isPossDupOrResend)
    {
        if (msgSeqNo != MISSING_INT)
        {
            proxy.heartbeat(testReqId, testReqIdLength, newSentSeqNum());
        }
        onMessage(msgSeqNo, isPossDupOrResend);
    }

    void onSequenceReset(final int msgSeqNo, final int newSeqNo, final boolean possDupFlag)
    {
        if (newSeqNo > msgSeqNo)
        {
            gapFill(msgSeqNo, newSeqNo, possDupFlag);
        }
        else
        {
            sequenceReset(msgSeqNo, newSeqNo);
        }
    }

    private void sequenceReset(final int msgSeqNo, final int newSeqNo)
    {
        final int expectedMsgSeqNo = expectedReceivedSeqNum();
        if (newSeqNo > expectedMsgSeqNo)
        {
            lastReceivedMsgSeqNum(newSeqNo - 1);
        }
        else if (newSeqNo < expectedMsgSeqNo)
        {
            proxy.reject(expectedMsgSeqNo, msgSeqNo);
        }
    }

    private void gapFill(final int msgSeqNo, final int newSeqNo, final boolean possDupFlag)
    {
        final int expectedMsgSeqNo = expectedReceivedSeqNum();
        if (msgSeqNo > expectedMsgSeqNo)
        {
            proxy.resendRequest(newSeqNo + 1, expectedMsgSeqNo, msgSeqNo - 1);
            lastReceivedMsgSeqNum(newSeqNo - 1);
        }
        else if (msgSeqNo < expectedMsgSeqNo)
        {
            if (!possDupFlag)
            {
                startLogout();
            }
        }
        else
        {
            lastReceivedMsgSeqNum(newSeqNo - 1);
        }
    }

    void onReject(final int msgSeqNo, final boolean isPossDupOrResend)
    {
        // TODO:
        onMessage(msgSeqNo, isPossDupOrResend);
    }

    public boolean onBeginString(final char[] value, final int length)
    {
        final boolean isValid = CodecUtil.equals(value, expectedBeginString, length);
        if (!isValid)
        {
            requestDisconnect();
        }
        return isValid;
    }

    // ---------- Accessors ----------

    long heartbeatIntervalInMs()
    {
        return this.heartbeatIntervalInMs;
    }

    long nextRequiredMessageTimeInMs()
    {
        return this.nextRequiredMessageTimeInMs;
    }

    Session heartbeatIntervalInS(final int heartbeatIntervalInS)
    {
        this.heartbeatIntervalInMs = MilliClock.fromSeconds(heartbeatIntervalInS);

        final long time = time();
        nextRequiredMessageTimeInMs = time + heartbeatIntervalInMs;
        sendingHeartbeatIntervalInMs = (long) (heartbeatIntervalInMs * HEARTBEAT_PAUSE_FACTOR);
        nextRequiredHeartbeatTimeInMs = time + sendingHeartbeatIntervalInMs;
        return this;
    }

    Session nextRequiredMessageTime(final long nextRequiredMessageTime)
    {
        this.nextRequiredMessageTimeInMs = nextRequiredMessageTime;
        return this;
    }

    public long id()
    {
        return id;
    }

    Session state(final SessionState state)
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

    public Session lastReceivedMsgSeqNum(final int value)
    {
        this.lastReceivedMsgSeqNum = value;
        receivedMsgSeqNo.setOrdered(value);
        return this;
    }

    public int expectedReceivedSeqNum()
    {
        return lastReceivedMsgSeqNum + 1;
    }

    public int sentSeqNum()
    {
        return lastSentMsgSeqNum;
    }

    public int newSentSeqNum()
    {
        final int lastSentMsgSeqNum = ++this.lastSentMsgSeqNum;
        sentMsgSeqNo.setOrdered(lastSentMsgSeqNum);
        return lastSentMsgSeqNum;
    }

    public void incReceivedSeqNum()
    {
        lastReceivedMsgSeqNum++;
        receivedMsgSeqNo.increment();
    }

    public int lastReceivedMsgSeqNum()
    {
        return lastReceivedMsgSeqNum;
    }

    public long connectionId()
    {
        return connectionId;
    }

}
