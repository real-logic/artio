/*
 * Copyright 2021 Monotonic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.binary_entrypoint;

import uk.co.real_logic.artio.fixp.FixPContext;

public class BinaryEntryPointContext implements FixPContext
{
    private final long sessionID;
    private long sessionVerID;
    private final long requestTimestamp;
    private final long enteringFirm;
    private final boolean fromNegotiate;

    private BinaryEntryPointKey key;

    public BinaryEntryPointContext(
        final long sessionID,
        final long sessionVerID,
        final long timestamp,
        final long enteringFirm,
        final boolean fromNegotiate)
    {
        this.sessionID = sessionID;
        this.sessionVerID = sessionVerID;
        this.requestTimestamp = timestamp;
        this.enteringFirm = enteringFirm;
        this.fromNegotiate = fromNegotiate;
    }

    public long sessionID()
    {
        return sessionID;
    }

    public long sessionVerID()
    {
        return sessionVerID;
    }

    public long requestTimestamp()
    {
        return requestTimestamp;
    }

    public long enteringFirm()
    {
        return enteringFirm;
    }

    public boolean fromNegotiate()
    {
        return fromNegotiate;
    }

    public String toString()
    {
        return "BinaryEntryPointContext{" +
            "sessionID=" + sessionID +
            ", sessionVerID=" + sessionVerID +
            ", requestTimestamp=" + requestTimestamp +
            ", enteringFirm=" + enteringFirm +
            ", fromNegotiate=" + fromNegotiate +
            '}';
    }

    public BinaryEntryPointKey toKey()
    {
        if (key == null)
        {
            key = new BinaryEntryPointKey(sessionID);
        }

        return key;
    }

    public boolean canAccept(final FixPContext fixPContext)
    {
        // Sanity checks
        if (!(fixPContext instanceof BinaryEntryPointContext))
        {
            return false;
        }

        final BinaryEntryPointContext otherContext = (BinaryEntryPointContext)fixPContext;
        if (sessionID != otherContext.sessionID)
        {
            return false;
        }

        // negotiations should increment the session ver id
        if (otherContext.fromNegotiate)
        {
            if (otherContext.sessionVerID == sessionVerID + 1)
            {
                sessionVerID++;
                return true;
            }
            else
            {
                return false;
            }
        }
        // establish messages shouldn't
        else
        {
            return otherContext.sessionVerID == sessionVerID;
        }
    }
}
