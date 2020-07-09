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
    private long uuid;
    private long lastUuid;
    private boolean newlyAllocated;
    private boolean primaryConnected;
    private boolean backupConnected;

    ILink3Context(final long uuid, final long lastUuid, final boolean newlyAllocated)
    {
        this.uuid = uuid;
        this.lastUuid = lastUuid;
        this.newlyAllocated = newlyAllocated;
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
}
