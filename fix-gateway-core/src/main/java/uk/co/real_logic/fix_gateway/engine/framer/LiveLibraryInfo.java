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
package uk.co.real_logic.fix_gateway.engine.framer;

import uk.co.real_logic.fix_gateway.LivenessDetector;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.framer.SubscriptionSlowPeeker.LibrarySlowPeeker;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Collections.unmodifiableList;

final class LiveLibraryInfo implements LibraryInfo
{
    private final int libraryId;
    private final LivenessDetector livenessDetector;
    private final int aeronSessionId;
    private final LibrarySlowPeeker librarySlowPeeker;
    private final List<GatewaySession> allSessions = new CopyOnWriteArrayList<>();
    private final List<SessionInfo> unmodifiableAllSessions = unmodifiableList(allSessions);
    private long acquireAtPosition;

    LiveLibraryInfo(
        final int libraryId,
        final LivenessDetector livenessDetector,
        final int aeronSessionId,
        final LibrarySlowPeeker librarySlowPeeker)
    {
        this.libraryId = libraryId;
        this.livenessDetector = livenessDetector;
        this.aeronSessionId = aeronSessionId;
        this.librarySlowPeeker = librarySlowPeeker;
    }

    public int libraryId()
    {
        return libraryId;
    }

    public List<SessionInfo> sessions()
    {
        return unmodifiableAllSessions;
    }

    public String toString()
    {
        return "LibraryInfo{" +
            "libraryId=" + libraryId +
            ", allSessions=" + allSessions +
            '}';
    }

    int aeronSessionId()
    {
        return aeronSessionId;
    }

    List<GatewaySession> gatewaySessions()
    {
        return allSessions;
    }

    void onHeartbeat(final long timeInMs)
    {
        livenessDetector.onHeartbeat(timeInMs);
    }

    int poll(final long timeInMs)
    {
        return livenessDetector.poll(timeInMs);
    }

    boolean isConnected()
    {
        return livenessDetector.isConnected();
    }

    void addSession(final GatewaySession session)
    {
        allSessions.add(session);
    }

    GatewaySession removeSession(final long connectionId)
    {
        return GatewaySessions.removeSessionByConnectionId(connectionId, allSessions);
    }

    void acquireAtPosition(final long libraryPosition)
    {
        acquireAtPosition = libraryPosition;
    }

    long acquireAtPosition()
    {
        return acquireAtPosition;
    }

    LibrarySlowPeeker librarySlowPeeker()
    {
        return librarySlowPeeker;
    }

    @Override
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

        final LiveLibraryInfo that = (LiveLibraryInfo) o;

        return libraryId == that.libraryId;
    }

    @Override
    public int hashCode()
    {
        return libraryId;
    }

    void releaseSlowPeeker()
    {
        librarySlowPeeker.removeLibrary();
    }
}
