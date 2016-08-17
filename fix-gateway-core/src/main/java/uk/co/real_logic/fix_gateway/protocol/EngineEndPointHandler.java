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
package uk.co.real_logic.fix_gateway.protocol;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.messages.SequenceNumberType;
import uk.co.real_logic.fix_gateway.messages.SessionState;

public interface EngineEndPointHandler
{
    Action onLibraryConnect(final int libraryId,
                            final long correlationId,
                            final int aeronSessionId);

    Action onInitiateConnection(
        final int libraryId,
        final int port,
        final String host,
        final String senderCompId,
        final String senderSubId,
        final String senderLocationId,
        final String targetCompId,
        final SequenceNumberType sequenceNumberType,
        final int requestedInitialSequenceNumber,
        final String username,
        final String password,
        final int heartbeatIntervalInS,
        final long correlationId,
        final Header header);

    Action onRequestDisconnect(
        final int libraryId,
        final long connectionId,
        final DisconnectReason reason);

    Action onApplicationHeartbeat(final int libraryId);

    Action onReleaseSession(
        final int libraryId,
        final long connectionId,
        final long correlationId,
        final SessionState state,
        final long heartbeatIntervalInMs,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final String username,
        final String password,
        final Header header);

    Action onRequestSession(
        final int libraryId,
        final long sessionId,
        final long correlationId,
        final int lastReceivedSequenceNumber);
}
