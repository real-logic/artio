/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.EngineDescriptorEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;

import static java.nio.charset.StandardCharsets.UTF_8;
import static uk.co.real_logic.fix_gateway.messages.EngineDescriptorEncoder.libraryChannelHeaderLength;
import static uk.co.real_logic.sbe.ir.generated.MessageHeaderEncoder.ENCODED_LENGTH;

final class EngineDescriptorFactory
{
    static DirectBuffer make(final String libraryChannel)
    {
        final byte[] libraryChannelBytes = libraryChannel.getBytes(UTF_8);
        final MessageHeaderEncoder messageHeader = new MessageHeaderEncoder();
        final EngineDescriptorEncoder descriptor = new EngineDescriptorEncoder();
        final int length = ENCODED_LENGTH + EngineDescriptorEncoder.BLOCK_LENGTH + libraryChannelHeaderLength() +
            libraryChannelBytes.length;

        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);

        descriptor
            .wrapAndApplyHeader(buffer, 0, messageHeader)
            .putLibraryChannel(libraryChannelBytes, 0, libraryChannelBytes.length);

        return buffer;
    }
}
