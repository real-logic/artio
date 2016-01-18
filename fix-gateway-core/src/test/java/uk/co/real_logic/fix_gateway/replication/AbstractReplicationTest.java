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
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;
import uk.co.real_logic.fix_gateway.TestFixtures;
import uk.co.real_logic.fix_gateway.engine.framer.ReliefValve;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveMetaData;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;
import uk.co.real_logic.fix_gateway.engine.logger.LogDirectoryDescriptor;

import java.io.File;

import static org.mockito.Mockito.mock;
import static uk.co.real_logic.agrona.CloseHelper.close;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.DEFAULT_LOGGER_CACHE_NUM_SETS;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.DEFAULT_LOGGER_CACHE_SET_SIZE;

public class AbstractReplicationTest
{

    protected static final String IPC = "aeron:ipc";
    protected static final int FRAGMENT_LIMIT = 1;
    protected static final long TIMEOUT = 100;
    protected static final long HEARTBEAT_INTERVAL = TIMEOUT / 2;
    protected static final int CLUSTER_SIZE = 3;
    protected static final long TIME = 0L;
    protected static final int DATA_SESSION_ID = 43;

    protected RaftNode raftNode1 = mock(RaftNode.class);
    protected RaftNode raftNode2 = mock(RaftNode.class);
    protected RaftNode raftNode3 = mock(RaftNode.class);

    protected TermState termState1 = new TermState();
    protected TermState termState2 = new TermState();
    protected TermState termState3 = new TermState();

    protected MediaDriver mediaDriver;
    protected Aeron aeron;

    protected Subscription controlSubscription()
    {
        return aeron.addSubscription(IPC, RaftNodeConfiguration.DEFAULT_CONTROL_STREAM_ID);
    }

    protected Subscription acknowledgementSubscription()
    {
        return aeron.addSubscription(IPC, RaftNodeConfiguration.DEFAULT_ACKNOWLEDGEMENT_STREAM_ID);
    }

    protected Subscription dataSubscription()
    {
        return aeron.addSubscription(IPC, RaftNodeConfiguration.DEFAULT_DATA_STREAM_ID);
    }

    protected RaftPublication raftPublication(final int streamId)
    {
        return new RaftPublication(
            100,
            new NoOpIdleStrategy(),
            mock(AtomicCounter.class),
            mock(ReliefValve.class),
            aeron.addPublication(IPC, streamId));
    }

    protected Publication dataPublication()
    {
        return aeron.addPublication(IPC, RaftNodeConfiguration.DEFAULT_DATA_STREAM_ID);
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
        deleteLogDir(1);
        deleteLogDir(2);
        deleteLogDir(3);

        close(aeron);
        close(mediaDriver);
    }

    private void deleteLogDir(final int id)
    {
        IoUtil.delete(new File(logFileDir((short) id)), true);
    }

    protected static int poll(final Role role)
    {
        return role.poll(FRAGMENT_LIMIT, 0);
    }

    protected static void poll1(final Role role)
    {
        while (role.poll(FRAGMENT_LIMIT, 0) == 0)
        {

        }
    }

    protected Follower follower(
        final short id,
        final RaftNode raftNode,
        final FragmentHandler handler,
        final TermState termState)
    {
        final ArchiveMetaData metaData = archiveMetaData(id);
        final Subscription subscription = dataSubscription();
        final StreamIdentifier streamId = new StreamIdentifier(subscription);
        final ArchiveReader archiveReader = new ArchiveReader(
            metaData, DEFAULT_LOGGER_CACHE_NUM_SETS, DEFAULT_LOGGER_CACHE_SET_SIZE, streamId);
        final Archiver archiver = new Archiver(
            metaData,
            DEFAULT_LOGGER_CACHE_NUM_SETS,
            DEFAULT_LOGGER_CACHE_SET_SIZE,
            streamId)
            .subscription(subscription);

        return new Follower(
            id,
            handler,
            raftNode,
            0,
            TIMEOUT,
            termState,
            archiveReader,
            archiver)
            .controlSubscription(controlSubscription())
            .acknowledgementPublication(raftPublication(RaftNodeConfiguration.DEFAULT_ACKNOWLEDGEMENT_STREAM_ID))
            .controlPublication(raftPublication(RaftNodeConfiguration.DEFAULT_CONTROL_STREAM_ID))
            .follow(0);
    }

    public static ArchiveMetaData archiveMetaData(final short nodeId)
    {
        final LogDirectoryDescriptor descriptor = new LogDirectoryDescriptor(logFileDir(nodeId));
        return new ArchiveMetaData(descriptor);
    }

    private static String logFileDir(final short id)
    {
        return IoUtil.tmpDirName() + "/node" + id;
    }

    protected static void run(final Role node1, final Role node2, final Role node3)
    {
        poll1(node1);
        poll1(node2);
        poll1(node3);

        //noinspection StatementWithEmptyBody
        while (poll(node1) + poll(node2) + poll(node3) > 0)
        {
        }
    }
}
