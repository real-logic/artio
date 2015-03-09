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
package uk.co.real_logic.fix_gateway.framer.session;

import uk.co.real_logic.agrona.DirectBuffer;

// TODO: move this to a use generated codecs once we've agreed on an API.
public class SessionParser
{
    private final Session session;
    private long sessionId;

    public SessionParser(final Session session)
    {
        this.session = session;
    }

    public long onMessage(final DirectBuffer buffer, final int offset, final int length, final long connectionId)
    {
        return connectionId;
    }
}
