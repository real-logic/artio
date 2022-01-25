/*
 * Copyright 2018-2022 Monotonic Ltd.
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
package uk.co.real_logic.artio.engine;

import org.agrona.DirectBuffer;

/**
 * A callback that can be implemented to inspect the messages that get replayed.
 *
 * This callback is called for every message that needs to be replayed, even those that are replaced with
 * a gap fill message. The handler is invoked on the Replay Agent.
 *
 * There is also the related {@link uk.co.real_logic.artio.session.ResendRequestController} which allows you to
 * accept or reject resend requests.
 */
@FunctionalInterface
public interface ReplayHandler
{
    /**
     * Event to indicate that a fix message has arrived to process.
     * @param buffer the buffer containing the fix message.
     * @param offset the offset in the buffer where the message starts.
     * @param length the length of the message within the buffer.
     * @param libraryId the id of the library which has received this message.
     * @param sessionId the id of the session which has received this message.
     * @param sequenceIndex the sequence index of the message being replayed.
     * @param messageType the FIX msgType field, encoded as an int.
     */
    void onReplayedMessage(
        DirectBuffer buffer,
        int offset,
        int length,
        int libraryId,
        long sessionId,
        int sequenceIndex,
        long messageType);
}
