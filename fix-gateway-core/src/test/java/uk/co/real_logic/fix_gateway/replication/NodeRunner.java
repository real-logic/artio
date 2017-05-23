/*
 * Copyright 2015-2017 Real Logic Ltd.
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

import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.TestFixtures;
import uk.co.real_logic.fix_gateway.engine.CompletionPosition;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveMetaData;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.CommonContext.AERON_DIR_PROP_DEFAULT;
import static io.aeron.driver.ThreadingMode.SHARED;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static org.agrona.BitUtil.SIZE_OF_SHORT;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static uk.co.real_logic.fix_gateway.TestFixtures.cleanupMediaDriver;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.DEFAULT_LOGGER_CACHE_NUM_SETS;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.DEFAULT_LOGGER_CACHE_SET_SIZE;
import static uk.co.real_logic.fix_gateway.replication.AbstractReplicationTest.logFileDir;
import static uk.co.real_logic.fix_gateway.replication.ClusterConfiguration.DEFAULT_DATA_STREAM_ID;
import static uk.co.real_logic.fix_gateway.replication.ReservedValue.NO_FILTER;

/**
 * Test Runner for an individual node, use single threaded.
 */
class NodeRunner implements AutoCloseable
{
    private static final long TIMEOUT_IN_MS = 300;
    private static final String AERON_CHANNEL = TestFixtures.clusteredAeronChannel();

    private final FrameDropper frameDropper;
    private final StashingRoleHandler stashingNodeHandler = new StashingRoleHandler();
    private final Int2IntHashMap nodeIdToId = new Int2IntHashMap(-1);
    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final ClusterAgent clusterAgent;
    private final NodeHandler handler;
    private final ClusterSubscription subscription;
    private final CompletionPosition completionPosition = new CompletionPosition();

    private final AtomicBoolean guard = new AtomicBoolean();
    private final ClusterablePublication publication;

    NodeRunner(final int nodeId, final int... otherNodes)
    {
        final File logFileDir = new File(logFileDir((short) nodeId));
        if (logFileDir.exists())
        {
            IoUtil.delete(logFileDir, true);
        }

        this.handler = new NodeHandler(nodeId);
        this.frameDropper = new FrameDropper(nodeId);

        final int termBufferLength = 1024 * 1024;
        final MediaDriver.Context context = new MediaDriver.Context()
            .threadingMode(SHARED)
            .sharedIdleStrategy(new YieldingIdleStrategy())
            .receiveChannelEndpointSupplier(frameDropper.newReceiveChannelEndpointSupplier())
            .sendChannelEndpointSupplier(frameDropper.newSendChannelEndpointSupplier())
            .dirsDeleteOnStart(true)
            .aeronDirectoryName(AERON_DIR_PROP_DEFAULT + nodeId)
            .publicationTermBufferLength(termBufferLength)
            .ipcTermBufferLength(termBufferLength);

        final IntHashSet otherNodeIds = new IntHashSet(40);
        for (final int node : otherNodes)
        {
            otherNodeIds.add(node);
        }

        mediaDriver = MediaDriver.launch(context);
        final Aeron.Context clientContext = new Aeron.Context()
            .aeronDirectoryName(context.aeronDirectoryName())
            .imageMapMode(READ_WRITE);
        aeron = Aeron.connect(clientContext);

        final StreamIdentifier dataStream = new StreamIdentifier(AERON_CHANNEL, DEFAULT_DATA_STREAM_ID);
        final ArchiveMetaData metaData = AbstractReplicationTest.archiveMetaData((short)nodeId);
        final ArchiveReader archiveReader = new ArchiveReader(
            metaData, DEFAULT_LOGGER_CACHE_NUM_SETS, DEFAULT_LOGGER_CACHE_SET_SIZE, dataStream, NO_FILTER);
        final Archiver archiver = new Archiver(
            metaData, DEFAULT_LOGGER_CACHE_NUM_SETS, DEFAULT_LOGGER_CACHE_SET_SIZE, dataStream, nodeId + "-",
            completionPosition);
        final UnsafeBuffer nodeState = new UnsafeBuffer(new byte[SIZE_OF_SHORT]);
        nodeState.putShort(0, (short)nodeId);

        final ClusterConfiguration configuration = new ClusterConfiguration()
            .nodeId((short)nodeId)
            .aeron(aeron)
            .otherNodes(otherNodeIds)
            .timeoutIntervalInMs(TIMEOUT_IN_MS)
            .failCounter(mock(AtomicCounter.class))
            .aeronChannel(AERON_CHANNEL)
            .archiver(archiver)
            .archiveReaderSupplier(() -> archiveReader)
            .nodeState(nodeState)
            .nodeStateHandler(new NodeIdStasher())
            .nodeHandler(stashingNodeHandler)
            .idleStrategy(new YieldingIdleStrategy());

        clusterAgent = new ClusterAgent(configuration, System.currentTimeMillis());
        subscription = clusterAgent.clusterStreams().subscription(1, "nodeRunner");
        publication = clusterAgent.clusterStreams().publication(1, "nodeRunner");
    }

    public void close()
    {
        completionPosition.complete(new Long2LongHashMap(-1));

        while (!guard.compareAndSet(false, true))
        {
            Thread.yield();
        }

        clusterAgent.onClose();
        CloseHelper.close(aeron);
        cleanupMediaDriver(mediaDriver);
    }

    public int poll(final int fragmentLimit)
    {
        int work = 0;

        if (guard.compareAndSet(false, true))
        {
            try
            {
                work += clusterAgent.doWork();
                work += subscription.poll(handler, fragmentLimit);

                validateRole();
            }
            finally
            {
                guard.set(false);
            }
        }

        return work;
    }

    void dropFrames(final boolean dropFrames)
    {
        frameDropper.dropFrames(dropFrames);
    }

    void dropFrames(final boolean dropInboundFrames, final boolean dropOutboundFrames)
    {
        frameDropper.dropFrames(dropInboundFrames, dropOutboundFrames);
    }

    boolean isLeader()
    {
        return clusterAgent.isLeader();
    }

    private void validateRole()
    {
        switch (stashingNodeHandler.role())
        {
            case LEADER:
                assertTrue(clusterAgent.isLeader());
                break;

            case FOLLOWER:
                assertTrue(clusterAgent.isFollower());
                break;

            case CANDIDATE:
                assertTrue(clusterAgent.isCandidate());
                break;
        }
    }

    public int leaderSessionId()
    {
        return termState().leaderSessionId().get();
    }

    private TermState termState()
    {
        return clusterAgent().termState();
    }

    int leadershipTerm()
    {
        return termState().leadershipTerm();
    }

    ClusterAgent clusterAgent()
    {
        return clusterAgent;
    }

    long replicatedPosition()
    {
        return handler.replicatedPosition();
    }

    void checkConsistencyOfReplicatedPositions()
    {
        handler.checkConsistencyOfReplicatedPositions();
    }

    Int2IntHashMap nodeIdToId()
    {
        return nodeIdToId;
    }

    long nodeId()
    {
        return clusterAgent().nodeId();
    }

    public ClusterablePublication publication()
    {
        return publication;
    }

    public long archivedPosition()
    {
        return clusterAgent.archivedPosition();
    }

    private class NodeIdStasher implements NodeStateHandler
    {
        public void onNewNodeState(
            final short nodeId,
            final int aeronSessionId,
            final DirectBuffer nodeStateBuffer,
            final int nodeStateLength)
        {
            if (nodeStateLength == SIZE_OF_SHORT)
            {
                nodeIdToId.put(nodeId, nodeStateBuffer.getShort(0));
            }
        }

        public void onNewLeader(final int leaderSessionId)
        {
        }

        public void noLeader()
        {
        }
    }
}
