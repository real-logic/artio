/*
 * Copyright 2021 Monotonic Ltd.
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

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.engine.logger.FixPSequenceNumberHandler;
import uk.co.real_logic.artio.messages.FixPMessageDecoder;
import uk.co.real_logic.artio.messages.FollowerSessionRequestDecoder;

/**
 * Class to implement by FIXP implementations in order to correctly index sequence numbers. Some FIXP protocols
 * (eg: iLink3) put sequence numbers inside messages, whilst others (eg: BinaryEntrypoint) use implicit sequence
 * numbers. This provides an abstract where different implementations can implement their choice.
 *
 * It's safe to assume that usage is single threaded.
 */
public abstract class AbstractFixPSequenceExtractor
{
    public static final long NEXT_SESSION_VERSION_ID = Long.MIN_VALUE;
    public static final int FORCE_START_REPLAY_CORR_ID = -1;

    protected final FixPSequenceNumberHandler handler;

    protected AbstractFixPSequenceExtractor(
        final FixPSequenceNumberHandler handler)
    {
        this.handler = handler;
    }

    public abstract void onMessage(
        FixPMessageDecoder fixPMessage,
        DirectBuffer buffer,
        int headerOffset,
        int totalLength,
        long endPosition,
        int aeronSessionId,
        long timestamp);

    // Only here for implementations to update their internal state, does not need to invoke handler.
    public abstract void onRedactSequenceUpdate(long sessionId, int newSequenceNumber);

    public abstract void onFollowerSessionRequest(
        FollowerSessionRequestDecoder followerSessionRequest,
        long endPosition,
        int totalLength,
        int aeronSessionId);
}
