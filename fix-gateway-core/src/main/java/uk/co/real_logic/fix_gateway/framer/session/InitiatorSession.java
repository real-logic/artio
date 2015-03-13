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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.builder.Encoder;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import static uk.co.real_logic.fix_gateway.framer.session.SessionState.CONNECTED;
import static uk.co.real_logic.fix_gateway.framer.session.SessionState.SENT_LOGON;

public class InitiatorSession extends Session
{
    private final FixGateway gateway;

    public InitiatorSession(
        final int heartbeatInterval,
        final long connectionId,
        final MilliClock clock,
        final SessionProxy proxy,
        final FixGateway gateway)
    {
        super(heartbeatInterval, connectionId, clock, CONNECTED, proxy);
        this.gateway = gateway;
    }

    void onLogon(final int heartbeatInterval, final int msgSeqNo, final long sessionId)
    {
        if (msgSeqNo == expectedSeqNo())
        {
            state(SessionState.ACTIVE);
            gateway.onInitiatorSessionActive(this);
        }
    }

    void poll()
    {
        if (state() == CONNECTED)
        {
            proxy.logon((int) (heartbeatIntervalInMs() / 1000), expectedSeqNo(), connectionId);
            state(SENT_LOGON);
        }
        super.poll();
    }

    public void send(final Encoder encoder)
    {

    }

    public void send(final DirectBuffer buffer, final int offset, final int length)
    {

    }

    public void logon()
    {

    }
}
