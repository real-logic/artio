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
import static uk.co.real_logic.fix_gateway.framer.session.SessionState.DISABLED;

/**
 * Stores information about the current state of a session - no matter whether outbound or inbound
 */
public abstract class Session
{
    public static final long UNKNOWN = -1;

    private final MilliClock clock;

    protected final SessionProxy proxy;
    protected final long connectionId;

    private long heartbeatIntervalInMs;
    private long nextRequiredMessageTime;
    private SessionState state;
    private long id = UNKNOWN;
    private int lastMsgSeqNum = 0;

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
        this.nextRequiredMessageTime = clock.time() + heartbeatIntervalInMs;
        this.connectionId = connectionId;
        this.state = state;
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
                proxy.resendRequest(expectedSeqNo, msgSeqNo - 1);
            }
            else if (expectedSeqNo > msgSeqNo)
            {
                disconnect();
            }
        }
    }

    abstract void onLogon(final int heartbeatInterval, final int msgSeqNo, final long sessionId);

    void onLogout(final int msgSeqNo, final long sessionId)
    {

        final int replySeqNo = msgSeqNo + 1;
        proxy.logout(replySeqNo, sessionId);
        lastMsgSeqNum(replySeqNo);

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
        final int expectedMsgSeqNo = expectedSeqNo();
        if (newSeqNo > expectedMsgSeqNo)
        {
            lastMsgSeqNum(newSeqNo - 1);
        }
        else if (newSeqNo < expectedMsgSeqNo)
        {
            proxy.reject(expectedMsgSeqNo, msgSeqNo);
        }
    }

    private void gapFill(final int msgSeqNo, final int newSeqNo, final boolean possDupFlag)
    {
        final int expectedMsgSeqNo = expectedSeqNo();
        if (msgSeqNo > expectedMsgSeqNo)
        {
            proxy.resendRequest(expectedMsgSeqNo, msgSeqNo - 1);
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

    void poll()
    {
        if (nextRequiredMessageTime() < time())
        {
            disconnect();
        }
    }

    protected void disconnect()
    {
        proxy.disconnect(connectionId);
        state(DISCONNECTED);
    }

    long heartbeatIntervalInMs()
    {
        return this.heartbeatIntervalInMs;
    }

    long nextRequiredMessageTime()
    {
        return this.nextRequiredMessageTime;
    }

    SessionState state()
    {
        return this.state;
    }

    Session heartbeatIntervalInMs(final int heartbeatIntervalInS)
    {
        this.heartbeatIntervalInMs = MilliClock.fromSeconds(heartbeatIntervalInS);
        return this;
    }

    Session nextRequiredMessageTime(final long nextRequiredMessageTime)
    {
        this.nextRequiredMessageTime = nextRequiredMessageTime;
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

    Session id(final long id)
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

    public boolean isConnected()
    {
        return state() != CONNECTING && state() != DISCONNECTED && state() != DISABLED;
    }

}
