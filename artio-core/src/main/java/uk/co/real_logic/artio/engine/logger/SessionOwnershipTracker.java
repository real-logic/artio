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
package uk.co.real_logic.artio.engine.logger;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import uk.co.real_logic.artio.messages.ManageSessionDecoder;

import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;

class SessionOwnershipTracker
{
    private static final long MISSING_SESSION = -1L;

    private final ManageSessionDecoder manageSession = new ManageSessionDecoder();
    private final Long2LongHashMap sessionIdToLibraryId = new Long2LongHashMap(MISSING_SESSION);

    private final RedactHandler redactHandler;
    private final boolean sent;

    SessionOwnershipTracker(
        final boolean sent, final RedactHandler redactHandler)
    {
        this.sent = sent;
        this.redactHandler = redactHandler;
    }

    void onManageSession(
        final DirectBuffer buffer, final int offset, final int actingBlockLength, final int version)
    {
        manageSession.wrap(buffer, offset, actingBlockLength, version);
        onManageSessionMessage();
    }

    private void onManageSessionMessage()
    {
        final int libraryId = manageSession.libraryId();
        final long sessionId = manageSession.session();
        final int lastSequenceNumber = sent ? manageSession.lastSentSequenceNumber() :
            manageSession.lastReceivedSequenceNumber();

        redactHandler.onRedact(sessionId, lastSequenceNumber);

        sessionIdToLibraryId.put(sessionId, libraryId);
    }

    boolean messageFromWrongLibrary(final long sessionId, final int libraryId)
    {
        final long expectedLibraryId = sessionIdToLibraryId.get(sessionId);
        return expectedLibraryId != MISSING_SESSION &&
            libraryId != expectedLibraryId && libraryId != ENGINE_LIBRARY_ID;
    }
}

interface RedactHandler
{
    void onRedact(long sessionId, int lastSequenceNumber);
}
