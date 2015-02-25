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
package uk.co.real_logic.fix_gateway.replication;

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.framer.MessageHandler;

/**
 * Publish data out
 *
 * TODO: figure out if this class should exist or if we just directly write to Aeron
 */
public final class ReplicationPublisher implements MessageHandler
{
    private final Publication dataPublication;

    public ReplicationPublisher(final Publication dataPublication)
    {
        this.dataPublication = dataPublication;
    }

    public void onMessage(final DirectBuffer buffer, final int offset, final int length, final long sessionId)
    {
        while (!dataPublication.offer(buffer, offset, length))
        {
            // TODO: backoff
        }
    }
}
