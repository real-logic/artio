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

    public void onLogin(final long heartbeatInterval, final int msgSeqNum, final long sessionId)
    {
        id(sessionId);

        final int expectedSeqNo = lastMsgSeqNum() + 1;
        if (expectedSeqNo == msgSeqNum)
        {
            heartbeatInterval(heartbeatInterval);
            state(ACTIVE);
            onMessage(msgSeqNum);
        }
        else if (expectedSeqNo < msgSeqNum)
        {
            heartbeatInterval(heartbeatInterval);
            state(AWAITING_RESEND);
            proxy.resendRequest(expectedSeqNo, msgSeqNum - 1);
        }
        else if (expectedSeqNo > msgSeqNum)
        {
            proxy.disconnect(connectionId());
            state(DISCONNECTED);
        }
    }

    public void onMessage(final int msgSeqNum)
    {
        lastMsgSeqNum(msgSeqNum);
    }

    public void onLogoutRequest()
    {

    }

    public void testRequest()
    {

    }

}
