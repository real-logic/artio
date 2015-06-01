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

import uk.co.real_logic.fix_gateway.replication.GatewayPublication;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import static uk.co.real_logic.fix_gateway.session.SessionState.CONNECTED;

public final class AcceptorSession extends Session
{

    public AcceptorSession(
        final int defaultInterval,
        final long connectionId,
        final MilliClock clock,
        final SessionProxy proxy,
        final GatewayPublication publication,
        final SessionIdStrategy sessionIdStrategy,
        final char[] beginString,
        final long sendingTimeWindow)
    {
        super(
            defaultInterval,
            connectionId,
            clock,
            CONNECTED,
            proxy,
            publication,
            sessionIdStrategy,
            beginString,
            sendingTimeWindow);
    }

    public void onLogon(
        final int heartbeatInterval,
        final int msgSeqNo,
        final long sessionId,
        final Object sessionKey,
        final long sendingTime)
    {
        if (state() == CONNECTED)
        {
            if (!validateHeartbeat(heartbeatInterval))
            {
                return;
            }

            id(sessionId);
            this.sessionKey = sessionKey;
            proxy.setupSession(sessionId, sessionKey);

            final int expectedSeqNo = expectedReceivedSeqNum();
            if (expectedSeqNo == msgSeqNo)
            {
                heartbeatIntervalInS(heartbeatInterval);
                state(SessionState.ACTIVE);
                replyToLogon(heartbeatInterval);
            }
            else if (expectedSeqNo < msgSeqNo)
            {
                state(SessionState.AWAITING_RESEND);
                replyToLogon(heartbeatInterval);
            }
            publication.saveConnect(connectionId, sessionId);
        }
        onMessage(msgSeqNo);
    }

    private void replyToLogon(int heartbeatInterval)
    {
        proxy.logon(heartbeatInterval, newSentSeqNum());
    }

}
