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
package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.CountersManager;

import java.net.SocketAddress;

public final class FixCounters
{
    private final CountersManager countersManager;
    private final AtomicCounter framerProxyFails;
    private final AtomicCounter failedInboundPublications;
    private final AtomicCounter failedOutboundPublications;
    private final AtomicCounter exceptions;

    public FixCounters(final CountersManager countersManager)
    {
        this.countersManager = countersManager;
        framerProxyFails = countersManager.newCounter("Failed offers to Framer Proxy");
        failedInboundPublications = countersManager.newCounter("Failed offer to inbound publication");
        failedOutboundPublications = countersManager.newCounter("Failed offer to outbound publication");
        exceptions = countersManager.newCounter("Gateway Exceptions");
    }

    public AtomicCounter framerProxyFails()
    {
        return framerProxyFails;
    }

    public AtomicCounter failedInboundPublications()
    {
        return failedInboundPublications;
    }

    public AtomicCounter failedOutboundPublications()
    {
        return failedOutboundPublications;
    }

    public AtomicCounter exceptions()
    {
        return exceptions;
    }

    public AtomicCounter messagesRead(final SocketAddress address)
    {
        return countersManager.newCounter("Messages Read from " + address);
    }

    public AtomicCounter messagesWritten(final SocketAddress address)
    {
        return countersManager.newCounter("Messages Written to " + address);
    }

    public AtomicCounter sentMsgSeqNo(final long connectionId)
    {
        return countersManager.newCounter("Last Sent MsgSeqNo for " + connectionId);
    }

    public AtomicCounter receivedMsgSeqNo(final long connectionId)
    {
        return countersManager.newCounter("Last Received MsgSeqNo for " + connectionId);
    }
}
