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
package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.framer.MessageHandler;

/**
 * A proxy for publishing messages fix related messages
 *
 */
public final class FixPublication implements MessageHandler
{
    private final Publication dataPublication;

    public FixPublication(final Publication dataPublication)
    {
        this.dataPublication = dataPublication;
    }

    public void onMessage(
        final DirectBuffer buffer, final int offset, final int length, final long sessionId, final int messageType)
    {
        // TODO: re-enable the framing once the optimal use of SBE is decided upon.
        /*messageFrame
            .messageType(ResendRequestDecoder.MESSAGE_TYPE)
            .session(sessionId)
            .connection(0L);*/

        while (!dataPublication.offer(buffer, offset, length))
        {
            // TODO: backoff
            // TODO: count failed retries similar to Aeron
        }
    }
}
