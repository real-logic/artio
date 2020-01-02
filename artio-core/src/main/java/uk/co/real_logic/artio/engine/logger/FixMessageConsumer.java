/*
 * Copyright 2015-2020 Real Logic Limited.
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

import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.messages.FixMessageDecoder;

/**
 * Consumer to read messages from the fix message archive.
 */
@FunctionalInterface
public interface FixMessageConsumer
{
    /**
     * Callback invoked for each message that the {@link FixArchiveScanner} scans.
     *
     * @param message the message header in the log file, can be used to read properties about the message.
     * @param buffer the buffer where the ascii FixMessage is stored.
     * @param offset the offset where the message begins within the buffer.
     * @param length the length of the FixMessage in bytes.
     * @param header the Aeron header object that can read properties about the framed Aeron message.
     */
    void onMessage(
        FixMessageDecoder message,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header);
}
