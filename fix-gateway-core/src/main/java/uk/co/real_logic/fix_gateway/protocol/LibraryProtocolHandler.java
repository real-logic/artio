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

import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.messages.ConnectionType;
import uk.co.real_logic.fix_gateway.messages.GatewayError;
import uk.co.real_logic.fix_gateway.messages.SessionReplyStatus;
import uk.co.real_logic.fix_gateway.messages.SessionState;

public interface LibraryProtocolHandler
{
    void onManageConnection(
        final int libraryId,
        final long connectionId,
        final ConnectionType type,
        final int lastSequenceNumber,
        final int lastReceivedSequenceNumber,
        final DirectBuffer buffer,
        final int addressOffset,
        final int addressLength,
        final SessionState state);

    void onConnect(
        final long connectionId,
        final String address);

    void onLogon(
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final String senderCompId,
        final String senderSubId,
        final String senderLocationId,
        final String targetCompId,
        final String username,
        final String password);

    void onError(final GatewayError errorType, final int libraryId, final String message);

    void onApplicationHeartbeat(final int libraryId);

    void onReleaseSessionReply(final long correlationId, final SessionReplyStatus status);

    void onRequestSessionReply(final long correlationId, final SessionReplyStatus status);

    void onCatchup(int libraryId, long connectionId, final int messageCount);

    void onNewSentPosition(long position);
}
