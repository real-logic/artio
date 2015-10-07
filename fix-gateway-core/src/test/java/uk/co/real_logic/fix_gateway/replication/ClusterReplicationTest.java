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

import org.junit.After;
import org.junit.Test;

import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static uk.co.real_logic.fix_gateway.replication.AbstractReplicationTest.poll;
import static uk.co.real_logic.fix_gateway.replication.AbstractReplicationTest.run;

/**
 * Test simulated cluster.
 */
public class ClusterReplicationTest
{

    private NodeRunner node1 = new NodeRunner(1, 2, 3);
    private NodeRunner node2 = new NodeRunner(2, 1, 3);
    private NodeRunner node3 = new NodeRunner(3, 1, 2);

    @Test(timeout = 10000)
    public void shouldEstablishCluster()
    {
        LockSupport.parkNanos(MILLISECONDS.toNanos(100));

        while (!foundLeader())
        {
            poll(node1);
            poll(node2);
            poll(node3);
            advanceAllClocks(10);
            LockSupport.parkNanos(1);
        }
    }

    private boolean foundLeader()
    {
        return node1.isLeader() || node2.isLeader() || node3.isLeader();
    }

    private void runCluster()
    {
        run(node1, node2, node3);
    }

    private void advanceAllClocks(final long delta)
    {
        node1.advanceClock(delta);
        node2.advanceClock(delta);
        node3.advanceClock(delta);
    }

    @After
    public void shutdown()
    {
        node1.close();
        node2.close();
        node3.close();
    }

}
