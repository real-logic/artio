/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.library;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.fixp.AbstractFixPParser;
import uk.co.real_logic.artio.messages.DisconnectReason;

class FixPSubscription
{
    private final AbstractFixPParser parser;
    private final InternalFixPConnection connection;

    FixPSubscription(final AbstractFixPParser parser, final InternalFixPConnection connection)
    {
        this.parser = parser;
        this.connection = connection;
    }

    public Action onMessage(final DirectBuffer buffer, final int offset)
    {
        return parser.onMessage(buffer, offset);
    }

    public long requestDisconnect(final DisconnectReason reason)
    {
        return connection.requestDisconnect(reason);
    }

    public Action onDisconnect(final DisconnectReason reason)
    {
        return connection.unbindState(reason);
    }

    public void onReplayComplete()
    {
        connection.onReplayComplete();
    }

    public InternalFixPConnection session()
    {
        return connection;
    }

    public boolean onThrottleNotification(
        final long refMsgType,
        final DirectBuffer businessRejectRefIDBuffer,
        final int businessRejectRefIDOffset,
        final int businessRejectRefIDLength)
    {
        return connection.onThrottleNotification(
            refMsgType, businessRejectRefIDBuffer, businessRejectRefIDOffset, businessRejectRefIDLength);
    }

    public long startEndOfDay()
    {
        return connection.startEndOfDay();
    }
}
