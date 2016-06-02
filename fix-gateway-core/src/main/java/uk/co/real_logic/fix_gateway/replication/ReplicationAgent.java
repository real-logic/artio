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
package uk.co.real_logic.fix_gateway.replication;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import uk.co.real_logic.fix_gateway.protocol.Streams;

// TODO: replication agent should copy another stream onto the cluster
public class ReplicationAgent implements Agent
{
    public static final int CONTROL_LIMIT = 10;

    //private final Subscription dataSubscription;
    //private final Subscription controlSubscription;
    private final FragmentHandler onDataMessageFunc = this::onDataMessage;

    public ReplicationAgent(
        final Streams streams)
    {
        //dataSubscription = streams.subscription();
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
        //dataSubscription.close();
        //controlSubscription.close();
    }

    public String roleName()
    {
        return "ReplicationAgent";
    }
}
