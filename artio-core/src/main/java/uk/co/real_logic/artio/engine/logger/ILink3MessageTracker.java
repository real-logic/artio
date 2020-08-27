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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.messages.ILinkMessageDecoder;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

public class ILink3MessageTracker extends MessageTracker
{
    public ILink3MessageTracker(final ControlledFragmentHandler messageHandler)
    {
        super(LogTag.REPLAY, messageHandler);
    }

    public Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        if (messageHeaderDecoder.templateId() == ILinkMessageDecoder.TEMPLATE_ID)
        {
            final Action action = messageHandler.onFragment(buffer, offset, length, header);
            if (action != ABORT)
            {
                count++;
            }

            return action;
        }

        return CONTINUE;
    }
}
