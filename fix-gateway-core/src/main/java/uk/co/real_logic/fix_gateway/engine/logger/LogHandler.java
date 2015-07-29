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
package uk.co.real_logic.fix_gateway.engine.logger;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;

public interface LogHandler
{
    /**
     * Callback to receive log entries in an archive reading/replay scenario.
     *
     * @param messageFrame the frame of the fix message.
     * @param srcBuffer the buffer where the message is stored.
     * @param startOffset the offset denoting the start of the messageFrame
     * @param messageOffset the offset denoting the start of the message body
     * @param messageLength the length of message body.
     * @return true if you want to continue scanning, false if you want to stop.
     */
    boolean onLogEntry(
        final FixMessageDecoder messageFrame,
        final DirectBuffer srcBuffer,
        final int startOffset,
        final int messageOffset,
        final int messageLength);
}
