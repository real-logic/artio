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
package uk.co.real_logic.artio.replication;

import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.ExclusiveBufferClaim;

class SoloPublication extends ClusterablePublication
{
    private final ExclusivePublication dataPublication;

    SoloPublication(final ExclusivePublication dataPublication)
    {
        this.dataPublication = dataPublication;
    }

    public long tryClaim(final int length, final ExclusiveBufferClaim bufferClaim)
    {
        return dataPublication.tryClaim(length, bufferClaim);
    }

    public void close()
    {
        dataPublication.close();
    }

    public int id()
    {
        return dataPublication.sessionId();
    }

    public long position()
    {
        return dataPublication.position();
    }

    public int maxPayloadLength()
    {
        return dataPublication.maxPayloadLength();
    }

    public String toString()
    {
        return "SoloPublication: " +
            dataPublication.channel() + "/" +
            dataPublication.streamId() + "/" +
            dataPublication.sessionId();
    }
}
