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
package uk.co.real_logic.fix_gateway.session_management;

/**
 * Stores information about the current state of a session - no matter whether outbound or inbound
 */
public final class SessionInformation
{
    public static final long UNKNOWN = -1;

    private long heartbeatInterval;
    private long nextRequiredMessageTime;
    private long connectionId;
    private long sequenceNumber;
    private SessionState state;

    public SessionInformation(
        final long heartbeatInterval,
        final long nextRequiredMessageTime,
        final long connectionId,
        final long sequenceNumber,
        final SessionState state)
    {
        this.heartbeatInterval = heartbeatInterval;
        this.nextRequiredMessageTime = nextRequiredMessageTime;
        this.connectionId = connectionId;
        this.sequenceNumber = sequenceNumber;
        this.state = state;
    }

    public long heartbeatInterval()
    {
        return this.heartbeatInterval;
    }

    public long nextRequiredMessageTime()
    {
        return this.nextRequiredMessageTime;
    }

    public long connectionId()
    {
        return this.connectionId;
    }

    public long sequenceNumber()
    {
        return this.sequenceNumber;
    }

    public SessionState state()
    {
        return this.state;
    }

    public SessionInformation heartbeatInterval(final long heartbeatInterval)
    {
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    public SessionInformation nextRequiredMessageTime(final long nextRequiredMessageTime)
    {
        this.nextRequiredMessageTime = nextRequiredMessageTime;
        return this;
    }

    public SessionInformation connectionId(final long connectionId)
    {
        this.connectionId = connectionId;
        return this;
    }

    public SessionInformation sequenceNumber(final long sequenceNumber)
    {
        this.sequenceNumber = sequenceNumber;
        return this;
    }

    public SessionInformation state(final SessionState state)
    {
        this.state = state;
        return this;
    }

}
