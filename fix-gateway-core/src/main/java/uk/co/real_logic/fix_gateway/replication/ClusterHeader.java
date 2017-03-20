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

/**
 * Performs similar role to Aeron's Header class, but for clusterable messages.
 *
 * At the moment they don't have a header
 */
public class ClusterHeader
{
    private final int streamId;
    private long position;
    private int sessionId;
    private byte flags;

    ClusterHeader(final int streamId)
    {
        this.streamId = streamId;
    }

    public final long position()
    {
        return position;
    }

    public int streamId()
    {
        return streamId;
    }

    void update(final long position, final int sessionId, final byte flags)
    {
        this.position = position;
        this.sessionId = sessionId;
        this.flags = flags;
    }

    // TODO: what does this mean?
    public int sessionId()
    {
        return sessionId;
    }

    public byte flags()
    {
        return flags;
    }
}
