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

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.agrona.collections.IntHashSet;

/**
 * .
 */
public class RaftNodeConfiguration
{
    private Aeron aeron;
    private StreamIdentifier controlStream;
    private StreamIdentifier dataStream;
    private StreamIdentifier acknowledgementStream;
    private short nodeId;
    private IntHashSet otherNodes;
    private long timeoutIntervalInMs;
    private AcknowledgementStrategy acknowledgementStrategy;
    private ReplicationHandler handler;

    public RaftNodeConfiguration controlStream(final StreamIdentifier controlStream)
    {
        this.controlStream = controlStream;
        return this;
    }

    public RaftNodeConfiguration dataStream(final StreamIdentifier dataStream)
    {
        this.dataStream = dataStream;
        return this;
    }

    public RaftNodeConfiguration acknowledgementStream(final StreamIdentifier acknowledgementStream)
    {
        this.acknowledgementStream = acknowledgementStream;
        return this;
    }

    public RaftNodeConfiguration nodeId(final short nodeId)
    {
        this.nodeId = nodeId;
        return this;
    }

    public RaftNodeConfiguration otherNodes(final IntHashSet otherNodes)
    {
        this.otherNodes = otherNodes;
        return this;
    }

    public RaftNodeConfiguration timeoutIntervalInMs(final long timeoutIntervalInMs)
    {
        this.timeoutIntervalInMs = timeoutIntervalInMs;
        return this;
    }

    public RaftNodeConfiguration acknowledgementStrategy(final AcknowledgementStrategy acknowledgementStrategy)
    {
        this.acknowledgementStrategy = acknowledgementStrategy;
        return this;
    }

    public RaftNodeConfiguration handler(final ReplicationHandler handler)
    {
        this.handler = handler;
        return this;
    }

    public RaftNodeConfiguration aeron(final Aeron aeron)
    {
        this.aeron = aeron;
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

    public ReplicationHandler handler()
    {
        return handler;
    }

    public Aeron aeron()
    {
        return aeron;
    }
}
