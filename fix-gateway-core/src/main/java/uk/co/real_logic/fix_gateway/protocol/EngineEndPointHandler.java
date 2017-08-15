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
package uk.co.real_logic.fix_gateway.protocol;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.messages.SequenceNumberType;
import uk.co.real_logic.fix_gateway.messages.SessionState;

public interface EngineEndPointHandler
{
    Action onLibraryConnect(int libraryId,
            String libraryName,
            long correlationId,
            int aeronSessionId);

    Action onInitiateConnection(
        int libraryId,
        int port,
        String host,
        String senderCompId,
        String senderSubId,
        String senderLocationId,
        String targetCompId,
        String targetSubId,
        String targetLocationId,
        SequenceNumberType sequenceNumberType,
        int requestedInitialSequenceNumber,
        boolean resetSequenceNumber,
        String username,
        String password,
        int heartbeatIntervalInS,
        long correlationId,
        Header header);

    Action onRequestDisconnect(int libraryId, long connectionId, DisconnectReason reason);

    /**
     * Update latest heartbeat time for the indicated library.
     * @param libraryId Library id
     * @param aeronSessionId Aeron session id
     * @return CONTINUE if successful, ABORT if not - ie if the library is already considered disconnected.
     */
    Action onApplicationHeartbeat(int libraryId, int aeronSessionId);

    Action onReleaseSession(
        int libraryId,
        long connectionId,
        long sessionId,
        long correlationId,
        SessionState state,
        long heartbeatIntervalInMs,
        int lastSentSequenceNumber,
        int lastReceivedSequenceNumber,
        String username,
        String password,
        Header header);

    Action onRequestSession(
        int libraryId,
        long sessionId,
        long correlationId,
        int lastReceivedSequenceNumber,
        int sequenceIndex);
}
