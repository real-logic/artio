/*
 * Copyright 2015-2020 Real Logic Limited.
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

import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.Session;

/**
 * Context information about a FIX session, that persists across restarts.
 */
class SessionContext implements SessionInfo
{
    private final CompositeKey compositeKey;
    private final long sessionId;
    private final SessionContexts sessionContexts;

    // onSequenceReset() will be called upon logon or not depending upon whether this is a persistent
    // session or not.
    // Variable only written to on the Framer thread but can be read on other threads via the
    // SessionInfo interface.
    private volatile int sequenceIndex;
    private final int initialSequenceIndex;

    private long lastLogonTime;
    private long lastSequenceResetTime;
    private FixDictionary lastFixDictionary;
    private int filePosition;

    SessionContext(
        final CompositeKey compositeKey,
        final long sessionId,
        final int sequenceIndex,
        final long lastLogonTime,
        final long lastSequenceResetTime,
        final SessionContexts sessionContexts,
        final int filePosition,
        final int initialSequenceIndex,
        final FixDictionary lastFixDictionary)
    {
        this.compositeKey = compositeKey;
        this.sessionId = sessionId;
        this.sequenceIndex = sequenceIndex;
        this.initialSequenceIndex = initialSequenceIndex;
        lastLogonTime(lastLogonTime);
        this.lastSequenceResetTime = lastSequenceResetTime;
        this.sessionContexts = sessionContexts;
        this.filePosition = filePosition;
        this.lastFixDictionary = lastFixDictionary;
    }

    private void lastLogonTime(final long lastLogonTime)
    {
        this.lastLogonTime = lastLogonTime;
    }

    void onSequenceReset(final long resetTime)
    {
        lastSequenceResetTime = resetTime;
        sequenceIndex = sequenceIndex == UNKNOWN_SEQUENCE_INDEX ? initialSequenceIndex : sequenceIndex + 1;
        save();
    }

    void updateAndSaveFrom(final Session session)
    {
        updateFrom(session);
        save();
    }

    void ensureFixDictionary(final FixDictionary fixDictionary)
    {
        if (lastFixDictionary != fixDictionary)
        {
            this.lastFixDictionary = fixDictionary;
            save();
        }
    }

    private void save()
    {
        sessionContexts.updateSavedData(this, filePosition);
    }

    void filePosition(final int filePosition)
    {
        this.filePosition = filePosition;
    }

    void updateFrom(final Session session)
    {
        sequenceIndex = session.sequenceIndex();
        lastLogonTime(session.lastLogonTime());
        lastSequenceResetTime = session.lastSequenceResetTime();
    }

    void onLogon(final boolean resetSeqNum, final long time, final FixDictionary fixDictionary)
    {
        lastFixDictionary = fixDictionary;
        lastLogonTime(time);
        // increment if we're going to reset the sequence number or if it's persistent
        // sequence numbers and it's the first time we're logging on.
        if (resetSeqNum || sequenceIndex == SessionContext.UNKNOWN_SEQUENCE_INDEX)
        {
            onSequenceReset(time);
        }
        else
        {
            // onSequenceReset also saves.
            save();
        }
    }

    public int sequenceIndex()
    {
        return sequenceIndex;
    }

    public long sessionId()
    {
        return sessionId;
    }

    public CompositeKey sessionKey()
    {
        return compositeKey;
    }

    public long lastSequenceResetTime()
    {
        return lastSequenceResetTime;
    }

    public long lastLogonTime()
    {
        return lastLogonTime;
    }

    public FixDictionary lastFixDictionary()
    {
        return lastFixDictionary;
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
