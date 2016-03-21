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

import uk.co.real_logic.fix_gateway.LivenessDetector;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Collections.unmodifiableList;

/**
 * Engine managed model of a library instance.
 */
public final class LibraryInfo
{
    private final boolean acceptor;
    private final int libraryId;
    private final LivenessDetector livenessDetector;
    private final List<GatewaySession> allSessions = new CopyOnWriteArrayList<>();
    private final List<SessionInfo> unmodifiableAllSessions = unmodifiableList(allSessions);

    LibraryInfo(final boolean acceptor, final int libraryId, final LivenessDetector livenessDetector)
    {
        this.acceptor = acceptor;
        this.libraryId = libraryId;
        this.livenessDetector = livenessDetector;
    }

    /**
     * Check whether the library is listed as the acceptor.
     *
     * @return true if the library is the acceptor, false otherwise.
     */
    public boolean isAcceptor()
    {
        return acceptor;
    }

    /**
     * Get the id of the library.
     *
     * @return the id of the library.
     */
    public int libraryId()
    {
        return libraryId;
    }

    /**
     * Get an unmodifiable list of the current sessions connected to this library.
     *
     * @return an unmodifiable list of the current sessions connected to this library.
     */
    public List<SessionInfo> sessions()
    {
        return unmodifiableAllSessions;
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

    public String toString()
    {
        return "LibraryInfo{" +
            "libraryId=" + libraryId +
            ", acceptor=" + acceptor +
            ", allSessions=" + allSessions +
            '}';
    }

    GatewaySession removeSession(final long connectionId)
    {
        return GatewaySessions.removeSession(connectionId, allSessions);
    }
}
