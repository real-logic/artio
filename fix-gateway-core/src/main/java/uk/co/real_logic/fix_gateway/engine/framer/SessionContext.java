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
package uk.co.real_logic.fix_gateway.engine.framer;

/**
 * Context information about a FIX session, that persists across restarts.
 */
class SessionContext
{
    static final int UNKNOWN = -1;

    private final long sessionId;
    // onSequenceReset() will be called upon the first logon
    private int sequenceIndex = UNKNOWN;

    // New session constructor
    SessionContext(final long sessionId)
    {
        this.sessionId = sessionId;
    }

    // Reload from disk constructor
    SessionContext(final long sessionId, final int sequenceIndex)
    {
        this.sessionId = sessionId;
        this.sequenceIndex = sequenceIndex;
    }

    void onSequenceReset()
    {
        sequenceIndex++;
    }

    int sequenceIndex()
    {
        return sequenceIndex;
    }

    long sessionId()
    {
        return sessionId;
    }

    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final SessionContext that = (SessionContext) o;

        return sessionId == that.sessionId;
    }

    public int hashCode()
    {
        return (int) (sessionId ^ (sessionId >>> 32));
    }

    public String toString()
    {
        return "SessionContext{" +
            "sessionId=" + sessionId +
            ", sequenceIndex=" + sequenceIndex +
            '}';
    }
}
