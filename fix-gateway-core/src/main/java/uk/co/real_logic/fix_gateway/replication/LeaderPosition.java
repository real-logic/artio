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

public class LeaderPosition
{
    private final short nodeId;
    private final int sessionId;
    private volatile long consensusPosition;

    public LeaderPosition(final short nodeId, final int sessionId)
    {
        this.nodeId = nodeId;
        this.sessionId = sessionId;
    }

    public short nodeId()
    {
        return nodeId;
    }

    public int sessionId()
    {
        return sessionId;
    }

    public long consensusPosition()
    {
        return consensusPosition;
    }

    public void consensusPosition(final long position)
    {
        this.consensusPosition = position;
    }
}
