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

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.fixp.AbstractBinaryParser;
import uk.co.real_logic.artio.messages.DisconnectReason;

class BinaryFixPSubscription
{
    private final AbstractBinaryParser parser;
    private final InternalBinaryFixPConnection connection;

    BinaryFixPSubscription(final AbstractBinaryParser parser, final InternalBinaryFixPConnection connection)
    {
        this.parser = parser;
        this.connection = connection;
    }

    public long onMessage(final DirectBuffer buffer, final int offset)
    {
        return parser.onMessage(buffer, offset);
    }

    public long requestDisconnect(final DisconnectReason reason)
    {
        return connection.requestDisconnect(reason);
    }

    public void onDisconnect(final DisconnectReason reason)
    {
        connection.unbindState(reason);
    }

    public void onReplayComplete()
    {
        connection.onReplayComplete();
    }

    public InternalBinaryFixPConnection session()
    {
        return connection;
    }
}
