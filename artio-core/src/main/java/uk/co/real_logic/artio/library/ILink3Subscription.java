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
import uk.co.real_logic.artio.ilink.AbstractILink3Parser;
import uk.co.real_logic.artio.messages.DisconnectReason;

class ILink3Subscription
{
    private final AbstractILink3Parser parser;
    private final ILink3Connection session;

    ILink3Subscription(final AbstractILink3Parser parser, final ILink3Connection session)
    {
        this.parser = parser;
        this.session = session;
    }

    public long onMessage(final DirectBuffer buffer, final int offset)
    {
        return parser.onMessage(buffer, offset);
    }

    public long requestDisconnect(final DisconnectReason reason)
    {
        return session.requestDisconnect(reason);
    }

    public void onDisconnect()
    {
        session.unbindState();
    }

    public void onReplayComplete()
    {
        session.onReplayComplete();
    }

    public ILink3Connection session()
    {
        return session;
    }
}
