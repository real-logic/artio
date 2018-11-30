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
package uk.co.real_logic.artio.engine.framer;

import uk.co.real_logic.artio.session.Session;

/**
 * Context information about a FIX session, that persists across restarts.
 */
class SessionContext
{
    static final int UNKNOWN_SEQUENCE_INDEX = -1;

    private final long sessionId;
    private final SessionContexts sessionContexts;
    private final int filePosition;

    // onSequenceReset() will be called upon logon or not depending upon whether this is a persistent
    // session or not.
    private int sequenceIndex;

    private long logonTime;

    SessionContext(
        final long sessionId,
        final int sequenceIndex,
        final long logonTime,
        final SessionContexts sessionContexts,
        final int filePosition)
    {
        this.sessionId = sessionId;
        this.sequenceIndex = sequenceIndex;
        this.logonTime = logonTime;
        this.sessionContexts = sessionContexts;
        this.filePosition = filePosition;
    }

    void onSequenceReset()
    {
        sequenceIndex++;
        sessionContexts.updateSavedData(filePosition, sequenceIndex, logonTime);
    }

    void updateAndSaveFrom(final Session session)
    {
        updateFrom(session);
        sessionContexts.updateSavedData(filePosition, sequenceIndex, logonTime);
    }

    void updateFrom(final Session session)
    {
        sequenceIndex = session.sequenceIndex();
        logonTime = session.logonTime();
    }

    void onLogon(final boolean resetSeqNum)
    {
        // increment if we're going to reset the sequence number or if it's persistent
        // sequence numbers and it's the first time we're logging on.
        if (resetSeqNum || sequenceIndex == SessionContext.UNKNOWN_SEQUENCE_INDEX)
        {
            onSequenceReset();
        }
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

        final SessionContext that = (SessionContext)o;

        return sessionId == that.sessionId;
    }

    public int hashCode()
    {
        return (int)(sessionId ^ (sessionId >>> 32));
    }

    public String toString()
    {
        return "SessionContext{" +
            "sessionId=" + sessionId +
            ", sequenceIndex=" + sequenceIndex +
            '}';
    }
}
