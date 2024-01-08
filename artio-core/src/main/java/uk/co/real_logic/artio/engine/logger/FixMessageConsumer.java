/*
 * Copyright 2015-2024 Real Logic Limited.
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

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.ArtioLogHeader;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.MessageStatus;

/**
 * Consumer to read messages from the fix message archive.
 */
@FunctionalInterface
public interface FixMessageConsumer
{
    /**
     * Callback invoked for each message that the {@link FixArchiveScanner} or {@link FixMessageLogger} scans.
     *
     * Note that not all of these messages are necessarily valid FIX messages. It is recommended that implementations
     * print the {@link FixMessageDecoder#status()} field in order to understand the message properly. For example
     * if a TCP connection is made to an Acceptor that sends some arbitrary binary payload from a misconfigured system
     * you will be given a callback here with that payload in marked as {@link MessageStatus#INVALID}. It can still
     * be valuable to print invalid messages in order to debug and understand data sent by malfunctioning clients of
     * your system.
     *
     * @param message the message header in the log file, can be used to read properties about the message.
     * @param buffer the buffer where the ascii FixMessage is stored.
     * @param offset the offset where the message begins within the buffer.
     * @param length the length of the FixMessage in bytes.
     * @param header a header object that can reflects properties of the original message before it was archived.
     */
    void onMessage(
        FixMessageDecoder message,
        DirectBuffer buffer,
        int offset,
        int length,
        ArtioLogHeader header);

    /**
     * Invoked when a scan begins, can be used to reset any internal state.
     */
    default void reset()
    {
    }
}
