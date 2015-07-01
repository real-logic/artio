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

import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import static uk.co.real_logic.fix_gateway.session.SessionState.*;

public class InitiatorSession extends Session
{
    public InitiatorSession(
        final int heartbeatInterval,
        final long connectionId,
        final MilliClock clock,
        final SessionProxy proxy,
        final GatewayPublication publication,
        final SessionIdStrategy sessionIdStrategy,
        final char[] beginString,
        final long sendingTimeWindow,
        final AtomicCounter receivedMsgSeqNo,
        final AtomicCounter sentMsgSeqNo)
    {
        super(
            heartbeatInterval,
            connectionId,
            clock,
            CONNECTED,
            proxy,
            publication,
            sessionIdStrategy,
            beginString,
            sendingTimeWindow,
            receivedMsgSeqNo,
            sentMsgSeqNo);
    }

    void onLogon(
        final int heartbeatInterval,
        final int msgSeqNo,
        final long sessionId,
        final Object sessionKey,
        final long sendingTime,
        final boolean isPossDupOrResend)
    {
        if (msgSeqNo == expectedReceivedSeqNum() && state() == SENT_LOGON)
        {
            state(ACTIVE);
            super.onLogon(heartbeatInterval, msgSeqNo, sessionId, sessionKey, sendingTime, isPossDupOrResend);
        }
        else
        {
            onMessage(msgSeqNo, isPossDupOrResend);
        }
    }

    public int poll(final long time)
    {
        int actions = 0;
        if (state() == CONNECTED && id() != UNKNOWN_ID)
        {
            state(SENT_LOGON);
            proxy.logon((int) (heartbeatIntervalInMs() / 1000), newSentSeqNum());
            actions++;
        }
        return actions + super.poll(time);
    }

}
