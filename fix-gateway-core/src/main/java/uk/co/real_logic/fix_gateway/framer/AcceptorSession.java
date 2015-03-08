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
package uk.co.real_logic.fix_gateway.framer;

import uk.co.real_logic.fix_gateway.util.MilliClock;

import static uk.co.real_logic.fix_gateway.framer.SessionState.*;

public final class AcceptorSession extends Session
{

    public AcceptorSession(
        final long defaultInterval, final long connectionId, final MilliClock clock, final SessionProxy proxy)
    {
        super(defaultInterval, clock, connectionId, UNKNOWN, CONNECTED, proxy);
    }

    public void onLogon(final long heartbeatInterval, final int msgSeqNo, final long sessionId)
    {
        id(sessionId);

        final int expectedSeqNo = expectedMsgSeqNo();
        if (expectedSeqNo == msgSeqNo)
        {
            heartbeatInterval(heartbeatInterval);
            state(ACTIVE);
            onMessage(msgSeqNo);
            proxy.logon(heartbeatInterval, msgSeqNo + 1, sessionId);
        }
        else if (expectedSeqNo < msgSeqNo)
        {
            heartbeatInterval(heartbeatInterval);
            state(AWAITING_RESEND);
            proxy.resendRequest(expectedSeqNo, msgSeqNo - 1);
        }
        else if (expectedSeqNo > msgSeqNo)
        {
            disconnect();
        }
    }

    public void onMessage(final int msgSeqNum)
    {
        if (state() == CONNECTED)
        {
            disconnect();
        }
        else
        {
            lastMsgSeqNum(msgSeqNum);
        }
    }

    public void onLogout(final int msgSeqNo, final long sessionId)
    {

        final int replySeqNo = msgSeqNo + 1;
        proxy.logout(replySeqNo, sessionId);
        lastMsgSeqNum(replySeqNo);

        disconnect();
    }

    public void onTestRequest(final String testReqId)
    {
        proxy.heartbeat(testReqId);
    }

    private void disconnect()
    {
        proxy.disconnect(connectionId);
        state(DISCONNECTED);
    }

    public void onSequenceReset(final int msgSeqNo, final int newSeqNo, final boolean possDupFlag)
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
        final int expectedMsgSeqNo = expectedMsgSeqNo();
        if (newSeqNo > expectedMsgSeqNo)
        {
            lastMsgSeqNum(newSeqNo - 1);
        }
        else if (newSeqNo < expectedMsgSeqNo)
        {
            proxy.reject(msgSeqNo);
        }
    }

    private void gapFill(final int msgSeqNo, final int newSeqNo, final boolean possDupFlag)
    {
        final int expectedMsgSeqNo = expectedMsgSeqNo();
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

}
