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
package uk.co.real_logic.artio.session;

import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.messages.ReplayMessagesStatus;
import uk.co.real_logic.artio.messages.ThrottleConfigurationStatus;

import java.util.function.BooleanSupplier;

/**
 * Decouples Session logic from either the Library or Engine process, used
 * to boostrap callbacks into either the Engine or the Library.
 */
public interface FixSessionOwner
{
    void onLogon(Session session);

    Reply<ReplayMessagesStatus> replayReceivedMessages(
        long sessionId,
        int replayFromSequenceNumber,
        int replayFromSequenceIndex,
        int replayToSequenceNumber,
        int replayToSequenceIndex,
        long timeout);

    // supplier returns true when it completes
    void enqueueTask(BooleanSupplier task);

    Reply<ThrottleConfigurationStatus> messageThrottle(
        long sessionId, int throttleWindowInMs, int throttleLimitOfMessages);

    long inboundMessagePosition();
}
