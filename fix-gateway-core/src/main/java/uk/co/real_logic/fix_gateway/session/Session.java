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
package uk.co.real_logic.fix_gateway.session;

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.Verify;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.builder.MessageEncoder;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;
import uk.co.real_logic.fix_gateway.util.MilliClock;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static uk.co.real_logic.fix_gateway.session.SessionState.*;

/**
 * Stores information about the current state of a session - no matter whether outbound or inbound
 */
public class Session
{
    public static final long UNKNOWN_ID = -1;

    /** The proportion of the maximum heartbeat interval before you send your heartbeat */
    public static final double HEARTBEAT_PAUSE_FACTOR = 0.8;

    private final MilliClock clock;

    protected final SessionProxy proxy;
    protected final long connectionId;
    protected final SessionIdStrategy sessionIdStrategy;
    protected final GatewayPublication publication;
    protected final MutableDirectBuffer buffer;
    protected final MutableAsciiFlyweight string;
    protected Object sessionKey;

    private SessionState state;
    private long id = UNKNOWN_ID;
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
        final SessionIdStrategy sessionIdStrategy)
    {
        Verify.notNull(clock, "clock");
        Verify.notNull(state, "session state");
        Verify.notNull(proxy, "session proxy");
        Verify.notNull(publication, "publication");

        this.clock = clock;
        this.proxy = proxy;
        this.connectionId = connectionId;
        this.publication = publication;
        this.sessionIdStrategy = sessionIdStrategy;

        buffer = new UnsafeBuffer(new byte[8 * 1024]);
        string = new MutableAsciiFlyweight(buffer);

        this.state = state;

        heartbeatIntervalInS(heartbeatIntervalInS);
    }

    // ---------- PUBLIC API ----------

    public boolean isConnected()
    {
        return state() != CONNECTING && state() != DISCONNECTED && state() != DISABLED;
    }

    public SessionState state()
    {
        return this.state;
    }

    public int poll(final long time)
    {
        int actions = 0;

        if (time >= nextRequiredMessageTimeInMs)
        {
            disconnect();
            actions++;
        }

        if (time >= nextRequiredHeartbeatTimeInMs)
        {
            proxy.heartbeat(null);
            nextRequiredHeartbeatTimeInMs += sendingHeartbeatIntervalInMs;
            actions++;
        }

        return actions;
    }

    public void disconnect()
    {
        proxy.logout(newSentSeqNum());
        proxy.disconnect(connectionId);
        state(DISCONNECTED);
        // TODO: await reply
    }

    public void send(final MessageEncoder encoder)
    {
        final HeaderEncoder header = (HeaderEncoder) encoder.header();
        header
            .msgSeqNum(newSentSeqNum())
            .sendingTime(time());
        // TODO: figure out the best way to remove this overhead from every send
        sessionIdStrategy.setupSession(sessionKey, header);

        final int length = encoder.encode(string, 0);

        publication.saveMessage(buffer, 0, length, id(), encoder.messageType());
    }

    public void send(
        final MutableDirectBuffer buffer,
        final int offset,
        final int length,
        final int messageType)
    {
        publication.saveMessage(buffer, offset, length, id(), messageType);
        newSentSeqNum();
    }

    // ---------- Event Handlers ----------

    void onMessage(final int msgSeqNo)
    {
        if (state() == CONNECTED)
        {
            disconnect();
        }
        else
        {
            final int expectedSeqNo = expectedReceivedSeqNum();
            if (expectedSeqNo == msgSeqNo)
            {
                nextRequiredMessageTime(time() + heartbeatIntervalInMs());
                lastReceivedMsgSeqNum(msgSeqNo);
            }
            else if (expectedSeqNo < msgSeqNo)
            {
                state(AWAITING_RESEND);
                proxy.resendRequest(newSentSeqNum(), expectedSeqNo, msgSeqNo - 1);
                incReceivedSeqNum();
            }
            else if (expectedSeqNo > msgSeqNo)
            {
                disconnect();
            }
        }
    }

    void onLogon(final int heartbeatInterval, final int msgSeqNo, final long sessionId, final Object sessionKey)
    {
        this.sessionKey = sessionKey;
        id(sessionId);
        heartbeatIntervalInS(heartbeatInterval);
        onMessage(msgSeqNo);
        publication.saveConnect(connectionId, sessionId);
        proxy.setupSession(sessionId, sessionKey);
    }

    void onLogout(final int msgSeqNo)
    {
        onMessage(msgSeqNo);
        newSentSeqNum();

        disconnect();
    }

    void onTestRequest(final String testReqId)
    {
        proxy.heartbeat(testReqId);
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
        else if(msgSeqNo < expectedMsgSeqNo)
        {
            if (!possDupFlag)
            {
                disconnect();
            }
        }
        else
        {
            lastReceivedMsgSeqNum(newSeqNo - 1);
        }
    }

    void onResendRequest(final int beginSeqNo, final int endSeqNo)
    {
        // TODO: decide how to resend messages once logging is figured out
    }

    void onReject()
    {
        // TODO
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

    Session lastReceivedMsgSeqNum(final int value)
    {
        this.lastReceivedMsgSeqNum = value;
        return this;
    }

    int expectedReceivedSeqNum()
    {
        return lastReceivedMsgSeqNum + 1;
    }

    int sentSeqNum()
    {
        return lastSentMsgSeqNum;
    }

    int newSentSeqNum()
    {
        return ++lastSentMsgSeqNum;
    }

    void incReceivedSeqNum()
    {
        lastReceivedMsgSeqNum++;
    }

}
