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

public final class AcceptorSession extends Session
{

    public AcceptorSession(
        final int defaultInterval, final long connectionId, final MilliClock clock, final SessionProxy proxy)
    {
        super(defaultInterval, connectionId, clock, SessionState.CONNECTED, proxy);
    }

    public void onLogon(final int heartbeatInterval, final int msgSeqNo, final long sessionId)
    {
        id(sessionId);

        final int expectedSeqNo = expectedSeqNo();
        if (expectedSeqNo == msgSeqNo)
        {
            heartbeatIntervalInMs(heartbeatInterval);
            state(SessionState.ACTIVE);
            proxy.logon(heartbeatInterval, msgSeqNo + 1, sessionId);
        }
        else if (expectedSeqNo < msgSeqNo)
        {
            state(SessionState.AWAITING_RESEND);
        }
    }

}
