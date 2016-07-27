/*
 * Copyright 2015-2016 Real Logic Ltd.
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

import org.agrona.CloseHelper;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

public class FixCounters implements AutoCloseable
{
    private final CountersManager countersManager;
    private final AtomicCounter failedInboundPublications;
    private final AtomicCounter failedOutboundPublications;
    private final AtomicCounter failedReplayPublications;

    private AtomicCounter failedRaftPublications = null;

    FixCounters(final CountersManager countersManager)
    {
        this.countersManager = countersManager;
        failedInboundPublications = countersManager.newCounter("Failed offer to inbound publication");
        failedOutboundPublications = countersManager.newCounter("Failed offer to outbound publication");
        failedReplayPublications = countersManager.newCounter("Failed offer to replay publication");
    }

    public AtomicCounter failedInboundPublications()
    {
        return failedInboundPublications;
    }

    public AtomicCounter failedOutboundPublications()
    {
        return failedOutboundPublications;
    }

    public AtomicCounter failedReplayPublications()
    {
        return failedReplayPublications;
    }

    public AtomicCounter failedRaftPublications()
    {
        if (failedRaftPublications == null)
        {
            failedRaftPublications = countersManager.newCounter("Failed offer to raft publication");
        }

        return failedRaftPublications;
    }

    public AtomicCounter messagesRead(final long connectionId, final String address)
    {
        return newCounter("Messages Read from " + address + " id = " + connectionId);
    }

    public AtomicCounter bytesInBuffer(final long connectionId, final String address)
    {
        return newCounter("Quarantined bytes for " + address + " id = " + connectionId);
    }

    public AtomicCounter invalidLibraryAttempts(final long connectionId, final String address)
    {
        return newCounter("Invalid Library Attempts for " + address + " id = " + connectionId);
    }

    public AtomicCounter sentMsgSeqNo(final long connectionId)
    {
        return newCounter("Last Sent MsgSeqNo for " + connectionId);
    }

    public AtomicCounter receivedMsgSeqNo(final long connectionId)
    {
        return newCounter("Last Received MsgSeqNo for " + connectionId);
    }

    private AtomicCounter newCounter(final String label)
    {
        return countersManager.newCounter(label);
    }

    public void close()
    {
        failedInboundPublications.close();
        failedOutboundPublications.close();
        failedReplayPublications.close();
        CloseHelper.close(failedRaftPublications);
    }

}
