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

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.FixPublication;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.builder.MessageEncoder;
import uk.co.real_logic.fix_gateway.util.MilliClock;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static uk.co.real_logic.fix_gateway.FixPublication.FRAME_SIZE;
import static uk.co.real_logic.fix_gateway.framer.session.SessionState.*;

public class InitiatorSession extends Session
{
    private final FixGateway gateway;
    private final FixPublication publication;
    private final SessionIdStrategy sessionIdStrategy;
    private final MutableDirectBuffer buffer;
    private final MutableAsciiFlyweight string;

    public InitiatorSession(
        final int heartbeatInterval,
        final long connectionId,
        final MilliClock clock,
        final SessionProxy proxy,
        final FixGateway gateway,
        final FixPublication publication,
        final long sessionId,
        final SessionIdStrategy sessionIdStrategy)
    {
        super(heartbeatInterval, connectionId, clock, CONNECTED, proxy);
        this.publication = publication;
        this.sessionIdStrategy = sessionIdStrategy;
        id(sessionId);
        this.gateway = gateway;
        buffer = new UnsafeBuffer(new byte[8 * 1024]);
        string = new MutableAsciiFlyweight(buffer);
    }

    void onLogon(final int heartbeatInterval, final int msgSeqNo, final long sessionId)
    {
        if (msgSeqNo == expectedSeqNo() && state() == SENT_LOGON)
        {
            state(ACTIVE);
            gateway.onInitiatorSessionActive(this);
        }
        onMessage(msgSeqNo);
    }

    public int poll(final long time)
    {
        int actions = 0;
        if (state() == CONNECTED)
        {
            state(SENT_LOGON);
            proxy.logon((int) (heartbeatIntervalInMs() / 1000), expectedSeqNo(), id());
            incrementSequenceNumber();
            actions++;
        }
        return actions + super.poll(time);
    }

    public void send(final MessageEncoder encoder)
    {
        final HeaderEncoder header = (HeaderEncoder) encoder.header();
        header.msgSeqNum(expectedSeqNo());
        sessionIdStrategy.encode(id(), header);

        final int length = encoder.encode(string, FRAME_SIZE);

        publication.onMessage(buffer, 0, length + FRAME_SIZE, id(), encoder.messageType());
        incrementSequenceNumber();
    }

}
