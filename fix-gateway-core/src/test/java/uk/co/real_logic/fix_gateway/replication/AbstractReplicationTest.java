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
import org.junit.Before;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;
import uk.co.real_logic.fix_gateway.TestFixtures;
import uk.co.real_logic.fix_gateway.engine.framer.ReliefValve;

import static org.mockito.Mockito.mock;
import static uk.co.real_logic.agrona.CloseHelper.close;

public class AbstractReplicationTest
{
    protected static final String IPC = "aeron:ipc";
    protected static final int CONTROL = 1;
    protected static final int FRAGMENT_LIMIT = 10;
    protected static final long REPLY_TIMEOUT = 100;
    protected static final long HEARTBEAT_INTERVAL = REPLY_TIMEOUT / 2;
    private static final int DATA = 2;

    protected Replicator replicator1 = mock(Replicator.class);
    protected Replicator replicator2 = mock(Replicator.class);
    protected Replicator replicator3 = mock(Replicator.class);

    protected MediaDriver mediaDriver;
    protected Aeron aeron;

    protected Subscription controlSubscription()
    {
        return aeron.addSubscription(IPC, CONTROL);
    }

    protected Subscription dataSubscription()
    {
        return aeron.addSubscription(IPC, DATA);
    }

    protected ControlPublication controlPublication()
    {
        return new ControlPublication(
            100,
            new NoOpIdleStrategy(),
            mock(AtomicCounter.class),
            mock(ReliefValve.class),
            aeron.addPublication(IPC, CONTROL));
    }

    protected Publication dataPublication()
    {
        return aeron.addPublication(IPC, DATA);
    }

    @Before
    public void setupAeron()
    {
        mediaDriver = TestFixtures.launchMediaDriver();
        aeron = Aeron.connect(new Aeron.Context());
    }

    @After
    public void teardownAeron()
    {
        close(aeron);
        close(mediaDriver);
    }

    protected int poll(final Role role)
    {
        return role.poll(FRAGMENT_LIMIT, 0);
    }
}
