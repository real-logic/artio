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
package uk.co.real_logic.fix_gateway.library.session;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.messages.ConnectionType;

public interface SessionHandler
{
    default void onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long connectionId,
        final long sessionId,
        final int messageType)
    {
        // Optional method, implement if you care about this type of message.
    }

    default void onDisconnect(final long connectionId)
    {
        // Optional method, implement if you care about this type of message.
    }

    default void onConnect(
        final int libraryId,
        final long connectionId,
        final ConnectionType type,
        final DirectBuffer buffer,
        final int addressOffset,
        final int addressLength)
    {
        // Optional method, implement if you care about this type of message.
    }

    default void onLogon(final long connectionId, final long sessionId)
    {
        // Optional method, implement if you care about this type of message.
    }

    default void onInitiateConnection(
        final int libraryId, final int port, final String host, final String senderCompId, final String targetCompId)
    {
        // Optional method, implement if you care about this type of message.
    }

    default void onRequestDisconnect(final long connectionId)
    {
        // Optional method, implement if you care about this type of message.
    }
}
