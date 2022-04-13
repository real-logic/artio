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
package uk.co.real_logic.artio.fixp;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;

public abstract class AbstractFixPParser
{
    public static final int FIXP_MESSAGE_HEADER_LENGTH = 8;
    public static final int BOOLEAN_FLAG_TRUE = 1;
    public static final int STANDARD_TEMPLATE_ID_OFFSET = 2;

    public abstract Action onMessage(DirectBuffer buffer, int offset);

    public abstract int templateId(DirectBuffer buffer, int offset);

    public abstract int blockLength(DirectBuffer buffer, int offset);

    public abstract int version(DirectBuffer buffer, int offset);

    public abstract FixPContext lookupContext(
        DirectBuffer messageBuffer,
        int messageOffset,
        int messageLength);

    public abstract long sessionId(DirectBuffer buffer, int offset);

    public int templateIdOffset()
    {
        return STANDARD_TEMPLATE_ID_OFFSET;
    }

    public abstract int retransmissionTemplateId();

    public abstract boolean isRetransmittedMessage(DirectBuffer buffer, int offset);

}
