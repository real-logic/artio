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
package uk.co.real_logic.fix_gateway.framer.session;

import uk.co.real_logic.fix_gateway.util.MilliClock;

import static uk.co.real_logic.fix_gateway.framer.session.SessionState.*;

/**
 * Stores information about the current state of a session - no matter whether outbound or inbound
 */
public class Session
{
    public static final long UNKNOWN_ID = -1;

    /** The proportion of the maximum heartbeat interval before you send your heartbeat */
    public static final double HEARTBEAT_PAUSE_FACTOR = 0.8;

    private final MilliClock clock;
    private final long sendingHeartbeatIntervalInMs;

    protected final SessionProxy proxy;
    protected final long connectionId;

    private long heartbeatIntervalInMs;
    private long nextRequiredMessageTimeInMs;
    private SessionState state;
    private long id = UNKNOWN_ID;
    private int lastMsgSeqNum = 0;
    private long nextRequiredHeartbeatTimeInMs;

    public Session(
            final int heartbeatIntervalInS,
            final long connectionId,
            final MilliClock clock,
            final SessionState state,
            final SessionProxy proxy)
    {
        heartbeatIntervalInMs(heartbeatIntervalInS);
        this.clock = clock;
        this.proxy = proxy;
        this.connectionId = connectionId;
        this.state = state;

        final long time = time();
        nextRequiredMessageTimeInMs = time + heartbeatIntervalInMs;
        sendingHeartbeatIntervalInMs = (long) (heartbeatIntervalInMs * HEARTBEAT_PAUSE_FACTOR);
        nextRequiredHeartbeatTimeInMs = time + sendingHeartbeatIntervalInMs;
    }

    public boolean isConnected()
    {
        return state() != CONNECTING && state() != DISCONNECTED && state() != DISABLED;
    }

    public SessionState state()
    {
        return this.state;
    }

    void onMessage(final int msgSeqNo)
    {
        if (state() == CONNECTED)
        {
            disconnect();
        }
        else
        {
            final int expectedSeqNo = expectedSeqNo();
            if (expectedSeqNo == msgSeqNo)
            {
                nextRequiredMessageTime(time() + heartbeatIntervalInMs());
                lastMsgSeqNum(msgSeqNo);
            }
            else if (expectedSeqNo < msgSeqNo)
            {
                state(AWAITING_RESEND);
                proxy.resendRequest(expectedSeqNo, msgSeqNo - 1, id());
            }
            else if (expectedSeqNo > msgSeqNo)
            {
                disconnect();
            }
        }
    }

    void onLogon(final int heartbeatInterval, final int msgSeqNo, final long sessionId)
    {
        lastMsgSeqNum(msgSeqNo);
        id(sessionId);
    }

    void onLogout(final int msgSeqNo, final long sessionId)
    {

        final int replySeqNo = msgSeqNo + 1;
        proxy.logout(replySeqNo, sessionId);
        lastMsgSeqNum(replySeqNo);

        disconnect();
    }

    void onTestRequest(final String testReqId)
    {
        proxy.heartbeat(testReqId, id());
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
        final int expectedMsgSeqNo = expectedSeqNo();
        if (newSeqNo > expectedMsgSeqNo)
        {
            lastMsgSeqNum(newSeqNo - 1);
        }
        else if (newSeqNo < expectedMsgSeqNo)
        {
            proxy.reject(expectedMsgSeqNo, msgSeqNo, id());
        }
    }

    private void gapFill(final int msgSeqNo, final int newSeqNo, final boolean possDupFlag)
    {
        final int expectedMsgSeqNo = expectedSeqNo();
        if (msgSeqNo > expectedMsgSeqNo)
        {
            proxy.resendRequest(expectedMsgSeqNo, msgSeqNo - 1, id());
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
            lastMsgSeqNum(newSeqNo - 1);
        }
    }

    void onResendRequest(final int beginSeqNo, final int endSeqNo)
    {
        // TODO: decide how to resend messages once logging is figured out
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
            proxy.heartbeat(null, id());
            nextRequiredHeartbeatTimeInMs += sendingHeartbeatIntervalInMs;
            actions++;
        }

        return actions;
    }

    public void disconnect()
    {
        proxy.disconnect(connectionId);
        state(DISCONNECTED);
    }

    long heartbeatIntervalInMs()
    {
        return this.heartbeatIntervalInMs;
    }

    long nextRequiredMessageTimeInMs()
    {
        return this.nextRequiredMessageTimeInMs;
    }

    Session heartbeatIntervalInMs(final int heartbeatIntervalInS)
    {
        this.heartbeatIntervalInMs = MilliClock.fromSeconds(heartbeatIntervalInS);
        return this;
    }

    Session nextRequiredMessageTime(final long nextRequiredMessageTime)
    {
        this.nextRequiredMessageTimeInMs = nextRequiredMessageTime;
        return this;
    }

    Session state(final SessionState state)
    {
        this.state = state;
        return this;
    }

    long id()
    {
        return id;
    }

    public Session id(final long id)
    {
        this.id = id;
        return this;
    }

    public int lastMsgSeqNo()
    {
        return lastMsgSeqNum;
    }

    Session lastMsgSeqNum(final int lastMsgSeqNum)
    {
        this.lastMsgSeqNum = lastMsgSeqNum;
        return this;
    }

    int expectedSeqNo()
    {
        return lastMsgSeqNo() + 1;
    }

    protected long time()
    {
        return clock.time();
    }

    public void onReject()
    {
        // TODO
    }
}
