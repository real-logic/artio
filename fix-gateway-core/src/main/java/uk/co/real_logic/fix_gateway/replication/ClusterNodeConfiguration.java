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

import io.aeron.Aeron;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;

import static uk.co.real_logic.fix_gateway.CommonConfiguration.backoffIdleStrategy;

public class ClusterNodeConfiguration
{
    public static final int DEFAULT_MAX_CLAIM_ATTEMPTS = 100_000;
    public static final int DEFAULT_CONTROL_STREAM_ID = 1;
    public static final int DEFAULT_DATA_STREAM_ID = 2;
    public static final int DEFAULT_ACKNOWLEDGEMENT_STREAM_ID = 3;

    private short nodeId;
    private IntHashSet otherNodes;
    private long timeoutIntervalInMs;

    private Aeron aeron;
    private StreamIdentifier controlStream;
    private StreamIdentifier dataStream;
    private StreamIdentifier acknowledgementStream;
    private IdleStrategy idleStrategy;
    private AcknowledgementStrategy acknowledgementStrategy;
    private ControlledFragmentHandler fragmentHandler;
    private int maxClaimAttempts = DEFAULT_MAX_CLAIM_ATTEMPTS;
    private AtomicCounter failCounter;
    private ArchiveReader archiveReader;
    private Archiver archiver;
    private RaftTransport raftTransport = new RaftTransport(this);

    /**
     * Sets the control, data and acknowledge streams to all this aeron
     * channel with their default ids.
     *
     * @param channel the aeron channel to use for all the streams
     * @return this
     */
    public ClusterNodeConfiguration aeronChannel(final String channel)
    {
        controlStream(new StreamIdentifier(channel, DEFAULT_CONTROL_STREAM_ID));
        dataStream(new StreamIdentifier(channel, DEFAULT_DATA_STREAM_ID));
        acknowledgementStream(new StreamIdentifier(channel, DEFAULT_ACKNOWLEDGEMENT_STREAM_ID));
        return this;
    }

    public ClusterNodeConfiguration controlStream(final StreamIdentifier controlStream)
    {
        this.controlStream = controlStream;
        return this;
    }

    public ClusterNodeConfiguration dataStream(final StreamIdentifier dataStream)
    {
        this.dataStream = dataStream;
        return this;
    }

    public ClusterNodeConfiguration acknowledgementStream(final StreamIdentifier acknowledgementStream)
    {
        this.acknowledgementStream = acknowledgementStream;
        return this;
    }

    public ClusterNodeConfiguration nodeId(final short nodeId)
    {
        this.nodeId = nodeId;
        return this;
    }

    public ClusterNodeConfiguration otherNodes(final IntHashSet otherNodes)
    {
        this.otherNodes = otherNodes;
        return this;
    }

    public ClusterNodeConfiguration timeoutIntervalInMs(final long timeoutIntervalInMs)
    {
        this.timeoutIntervalInMs = timeoutIntervalInMs;
        return this;
    }

    public ClusterNodeConfiguration acknowledgementStrategy(final AcknowledgementStrategy acknowledgementStrategy)
    {
        this.acknowledgementStrategy = acknowledgementStrategy;
        return this;
    }

    public ClusterNodeConfiguration fragmentHandler(final ControlledFragmentHandler fragmentHandler)
    {
        this.fragmentHandler = fragmentHandler;
        return this;
    }

    public ClusterNodeConfiguration aeron(final Aeron aeron)
    {
        this.aeron = aeron;
        return this;
    }

    public ClusterNodeConfiguration idleStrategy(final IdleStrategy idleStrategy)
    {
        this.idleStrategy = idleStrategy;
        return this;
    }

    public ClusterNodeConfiguration maxClaimAttempts(final int maxClaimAttempts)
    {
        this.maxClaimAttempts = maxClaimAttempts;
        return this;
    }

    public ClusterNodeConfiguration failCounter(final AtomicCounter failCounter)
    {
        this.failCounter = failCounter;
        return this;
    }

    public ClusterNodeConfiguration archiveReader(final ArchiveReader archiveReader)
    {
        this.archiveReader = archiveReader;
        return this;
    }

    public ClusterNodeConfiguration archiver(final Archiver archiver)
    {
        this.archiver = archiver;
        return this;
    }

    public ClusterNodeConfiguration raftTransport(final RaftTransport raftTransport)
    {
        this.raftTransport = raftTransport;
        return this;
    }

    public StreamIdentifier controlStream()
    {
        return controlStream;
    }

    public StreamIdentifier dataStream()
    {
        return dataStream;
    }

    public StreamIdentifier acknowledgementStream()
    {
        return acknowledgementStream;
    }

    public short nodeId()
    {
        return nodeId;
    }

    public IntHashSet otherNodes()
    {
        return otherNodes;
    }

    public long timeoutIntervalInMs()
    {
        return timeoutIntervalInMs;
    }

    public AcknowledgementStrategy acknowledgementStrategy()
    {
        return acknowledgementStrategy;
    }

    public ControlledFragmentHandler fragmentHandler()
    {
        return fragmentHandler;
    }

    public Aeron aeron()
    {
        return aeron;
    }

    public IdleStrategy idleStrategy()
    {
        return idleStrategy;
    }

    public int maxClaimAttempts()
    {
        return maxClaimAttempts;
    }

    public AtomicCounter failCounter()
    {
        return failCounter;
    }

    public ArchiveReader archiveReader()
    {
        return archiveReader;
    }

    public Archiver archiver()
    {
        return archiver;
    }

    public RaftTransport raftTransport()
    {
        return raftTransport;
    }

    public void conclude()
    {
        if (idleStrategy() == null)
        {
            idleStrategy(backoffIdleStrategy());
        }

        if (acknowledgementStrategy() == null)
        {
            acknowledgementStrategy(new QuorumAcknowledgementStrategy());
        }
    }
}
