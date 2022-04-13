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
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.util.CharFormatter;

public abstract class MessageTracker implements ControlledFragmentHandler
{
    static final ThreadLocal<CharFormatter> FOUND_REPLAY_MESSAGE =
        ThreadLocal.withInitial(() -> new CharFormatter("Found Replay Message [%s]"));

    final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    final LogTag logTag;
    final ControlledFragmentHandler messageHandler;

    int maxCount;
    int count;

    MessageTracker(final LogTag logTag, final ControlledFragmentHandler messageHandler)
    {
        this.logTag = logTag;
        this.messageHandler = messageHandler;
    }

    void reset(final int maxCount)
    {
        this.maxCount = maxCount;
        this.count = 0;
    }
}
