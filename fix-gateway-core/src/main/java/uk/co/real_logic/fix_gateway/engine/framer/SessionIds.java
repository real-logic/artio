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
package uk.co.real_logic.fix_gateway.engine.framer;

import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.collections.LongHashSet;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.fix_gateway.engine.logger.LoggerUtil;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static uk.co.real_logic.fix_gateway.messages.SessionIdEncoder.BLOCK_LENGTH;
import static uk.co.real_logic.fix_gateway.session.SessionIdStrategy.INSUFFICIENT_SPACE;

/**
 * Identifies which sessions are currently authenticated.
 */
public class SessionIds
{
    public static final long MISSING = -2;
    public static final long DUPLICATE_SESSION = -1;

    private static final int HEADER_SIZE = MessageHeaderDecoder.ENCODED_LENGTH;

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final SessionIdEncoder sessionIdEncoder = new SessionIdEncoder();
    private final int actingBlockLength = sessionIdEncoder.sbeBlockLength();
    private final int actingVersion = sessionIdEncoder.sbeSchemaVersion();

    private final Function<Object, Long> onLogonFunc = this::onNewLogon;
    private final LongHashSet currentlyAuthenticated = new LongHashSet(MISSING);
    private final Map<Object, Long> compositeToSurrogate = new HashMap<>();

    private final AtomicBuffer buffer;
    private final SessionIdStrategy idStrategy;
    private final ErrorHandler errorHandler;

    private static long counter = 1L;

    private int bufferPosition;

    // TODO: allocate a sparse memory mapped file, set a length, do a force
    // TODO: think about length/remap for growth
    // TODO: add administrative reset operation that uses a new file
    public SessionIds(
        final AtomicBuffer buffer, final SessionIdStrategy idStrategy, final ErrorHandler errorHandler)
    {
        this.buffer = buffer;
        this.idStrategy = idStrategy;
        this.errorHandler = errorHandler;
        loadBuffer();
    }

    private void loadBuffer()
    {
        LoggerUtil.initialiseBuffer(
            buffer,
            headerEncoder,
            headerDecoder,
            sessionIdEncoder.sbeSchemaId(),
            sessionIdEncoder.sbeTemplateId(),
            actingVersion,
            actingBlockLength);

        final SessionIdDecoder sessionIdDecoder = new SessionIdDecoder();

        bufferPosition = HEADER_SIZE;
        final int lastRecordStart = buffer.capacity() - BLOCK_LENGTH;
        while (bufferPosition < lastRecordStart)
        {
            sessionIdDecoder.wrap(buffer, bufferPosition, actingBlockLength, actingVersion);

            final long sessionId = sessionIdDecoder.sessionId();
            if (sessionId == 0)
            {
                return;
            }

            final int compositeKeyLength = sessionIdDecoder.compositeKeyLength();
            final Object compositeKey = idStrategy.load(
                buffer, bufferPosition + BLOCK_LENGTH, compositeKeyLength);
            if (compositeKey == null)
            {
                return;
            }

            compositeToSurrogate.put(compositeKey, sessionId);

            bufferPosition += BLOCK_LENGTH + compositeKeyLength;
        }
    }

    public long onLogon(final Object compositeKey)
    {
        final Long sessionId = compositeToSurrogate.computeIfAbsent(compositeKey, onLogonFunc);

        if (!currentlyAuthenticated.add(sessionId))
        {
            return DUPLICATE_SESSION;
        }

        return sessionId;
    }

    private long onNewLogon(final Object compositeKey)
    {
        final long sessionId = counter++;
        // TODO: save to an in memory buffer, then copy the buffer down
        // TODO: don't allow records to span a block, no padding needed, just scan forward
        // TODO: assume 4K block size
        // TODO: add CRC32 per block, per at end, update on every write
        final int compositeKeyLength = idStrategy.save(
            compositeKey, buffer, bufferPosition + BLOCK_LENGTH);

        if (compositeKeyLength == INSUFFICIENT_SPACE)
        {
            errorHandler.onError(new IllegalStateException(String.format(
                "Unable to save record session id %d for %s due to insufficient space",
                sessionId,
                compositeKey)));
        }
        else
        {
            sessionIdEncoder
                .wrap(buffer, bufferPosition)
                .sessionId(sessionId)
                .compositeKeyLength(compositeKeyLength);

            bufferPosition += BLOCK_LENGTH + compositeKeyLength;
        }

        return sessionId;
    }

    public void onDisconnect(final long sessionId)
    {
        currentlyAuthenticated.remove(sessionId);
    }

}
