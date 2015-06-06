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

import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;

public class Follower implements FragmentHandler
{
    private final int id;
    private final ControlProtocol controlProtocol;
    private final DataSubscriber delegate;

    public Follower(
        final int id, final ControlProtocol controlProtocol, final DataSubscriber delegate)
    {
        this.id = id;
        this.controlProtocol = controlProtocol;
        this.delegate = delegate;
    }

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        controlProtocol.messageAcknowledgement(header.position(), id);
        delegate.onFragment(buffer, offset, length, header);
    }
}
