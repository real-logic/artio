/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.engine.framer;

final class ILink3Context
{
    private final int offset;

    // holds the last uuid whilst we're connecting but before we're sure that we've connected
    private long connectLastUuid;
    private long connectUuid;

    private final ILink3Contexts iLink3Contexts;
    private long uuid;
    private long lastUuid;
    private boolean newlyAllocated;
    private boolean primaryConnected;
    private boolean backupConnected;

    ILink3Context(
        final ILink3Contexts iLink3Contexts,
        final long uuid,
        final long lastUuid,
        final long connectUuid,
        final long connectLastUuid,
        final boolean newlyAllocated,
        final int offset)
    {
        this.iLink3Contexts = iLink3Contexts;
        this.uuid = uuid;
        this.lastUuid = lastUuid;
        this.connectLastUuid = connectLastUuid;
        this.connectUuid = connectUuid;
        this.newlyAllocated = newlyAllocated;
        this.offset = offset;
    }

    long uuid()
    {
        return uuid;
    }

    void uuid(final long uuid)
    {
        this.uuid = uuid;
    }

    long lastUuid()
    {
        return lastUuid;
    }

    void lastUuid(final long lastUuid)
    {
        this.lastUuid = lastUuid;
    }

    boolean newlyAllocated()
    {
        return newlyAllocated;
    }

    void newlyAllocated(final boolean newlyAllocated)
    {
        this.newlyAllocated = newlyAllocated;
    }

    boolean primaryConnected()
    {
        return primaryConnected;
    }

    public void primaryConnected(final boolean connected)
    {
        this.primaryConnected = connected;
    }

    boolean backupConnected()
    {
        return backupConnected;
    }

    public void backupConnected(final boolean connected)
    {
        this.backupConnected = connected;
    }

    public int offset()
    {
        return offset;
    }

    public void confirmUuid()
    {
        uuid = connectUuid;
        lastUuid = connectLastUuid;

        if (lastUuid == 0)
        {
            iLink3Contexts.saveNewUuid(this);
        }
        else
        {
            iLink3Contexts.updateUuid(this);
        }
    }

    public void connectLastUuid(final long uuid)
    {
        this.connectLastUuid = uuid;
    }

    public long connectLastUuid()
    {
        return connectLastUuid;
    }

    public void connectUuid(final long newUuid)
    {
        connectUuid = newUuid;
    }

    public long connectUuid()
    {
        return connectUuid;
    }
}
