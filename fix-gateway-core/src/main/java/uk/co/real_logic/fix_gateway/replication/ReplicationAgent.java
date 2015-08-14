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
package uk.co.real_logic.fix_gateway.replication;

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.fix_gateway.streams.ReplicatedStream;

public class ReplicationAgent implements Agent
{
    public static final int CONTROL_LIMIT = 10;

    private final Subscription dataSubscription;
    //private final Subscription controlSubscription;
    private final FragmentHandler onDataMessageFunc = this::onDataMessage;

    public ReplicationAgent(
        final ReplicatedStream replicatedStream)
    {
        dataSubscription = replicatedStream.dataSubscription();
        //controlSubscription = replicatedStream.controlSubscription();
    }

    private void onDataMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {

    }

    public int doWork() throws Exception
    {
        return 0; //controlSubscription.poll(onDataMessageFunc, CONTROL_LIMIT);
    }

    public void onClose()
    {
        dataSubscription.close();
        //controlSubscription.close();
    }

    public String roleName()
    {
        return "ReplicationAgent";
    }
}
