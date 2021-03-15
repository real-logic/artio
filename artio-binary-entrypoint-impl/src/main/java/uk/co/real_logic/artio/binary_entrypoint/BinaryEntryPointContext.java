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
    private final long sessionVerID;
    private final long requestTimestamp;
    private final long enteringFirm;

    public BinaryEntryPointContext(
        final long sessionID, final long sessionVerID, final long timestamp, final long enteringFirm)
    {
        this.sessionID = sessionID;
        this.sessionVerID = sessionVerID;
        this.requestTimestamp = timestamp;
        this.enteringFirm = enteringFirm;
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
}
