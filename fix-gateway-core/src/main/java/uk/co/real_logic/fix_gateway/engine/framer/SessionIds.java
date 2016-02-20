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
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.FileSystemCorruptionException;
import uk.co.real_logic.fix_gateway.engine.logger.LoggerUtil;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.messages.SessionIdDecoder;
import uk.co.real_logic.fix_gateway.messages.SessionIdEncoder;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.zip.CRC32;

import static uk.co.real_logic.fix_gateway.StorageDescriptor.*;
import static uk.co.real_logic.fix_gateway.messages.SessionIdEncoder.BLOCK_LENGTH;
import static uk.co.real_logic.fix_gateway.session.SessionIdStrategy.INSUFFICIENT_SPACE;

/**
 * Identifies which sessions are currently authenticated.
 *
 * The session ids table is saved into a file. Records are written out using the {@link SessionIdEncoder}
 * and aren't allowed to span sectors. Each sector has a CRC32 checksum and each checksum is updated after writing
 * each session id record.
 */
public class SessionIds
{
    public static final long MISSING = -2;
    public static final long DUPLICATE_SESSION = -1;

    private static final int HEADER_SIZE = MessageHeaderDecoder.ENCODED_LENGTH;

    private static final int ENCODING_BUFFER_SIZE = SECTOR_SIZE - CHECKSUM_SIZE;
    private final UnsafeBuffer compositeKeyBuffer = new UnsafeBuffer(new byte[ENCODING_BUFFER_SIZE]);

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final SessionIdEncoder sessionIdEncoder = new SessionIdEncoder();
    private final int actingBlockLength = sessionIdEncoder.sbeBlockLength();
    private final int actingVersion = sessionIdEncoder.sbeSchemaVersion();

    private final Function<Object, Long> onLogonFunc = this::onNewLogon;
    private final LongHashSet currentlyAuthenticated = new LongHashSet(MISSING);
    private final Map<Object, Long> compositeToSurrogate = new HashMap<>();

    private static final int FIRST_CHECKSUM_LOCATION = SECTOR_SIZE - CHECKSUM_SIZE;
    private final CRC32 crc32 = new CRC32();
    private final ByteBuffer byteBuffer;

    private final AtomicBuffer buffer;
    private final SessionIdStrategy idStrategy;
    private final ErrorHandler errorHandler;

    private static long counter = 1L;

    private int filePosition;

    // TODO: allocate a sparse memory mapped file, set a length, do a force
    // TODO: add administrative reset operation that uses a new file
    public SessionIds(
        final AtomicBuffer buffer, final SessionIdStrategy idStrategy, final ErrorHandler errorHandler)
    {
        this.buffer = buffer;
        this.byteBuffer = buffer.byteBuffer();
        this.idStrategy = idStrategy;
        this.errorHandler = errorHandler;
        loadBuffer();
    }

    private void loadBuffer()
    {
        if (byteBuffer == null)
        {
            throw new IllegalStateException("Must use atomic buffer backed by a byte buffer");
        }

        if (LoggerUtil.initialiseBuffer(
            buffer,
            headerEncoder,
            headerDecoder,
            sessionIdEncoder.sbeSchemaId(),
            sessionIdEncoder.sbeTemplateId(),
            actingVersion,
            actingBlockLength))
        {
            byteBuffer.position(0).limit(FIRST_CHECKSUM_LOCATION);
            updateChecksum(FIRST_CHECKSUM_LOCATION);
        }

        final SessionIdDecoder sessionIdDecoder = new SessionIdDecoder();

        int sectorEnd = 0;
        filePosition = HEADER_SIZE;
        final int lastRecordStart = buffer.capacity() - BLOCK_LENGTH;
        while (filePosition < lastRecordStart)
        {
            sectorEnd = validateSectorChecksum(filePosition, sectorEnd);
            sessionIdDecoder.wrap(buffer, filePosition, actingBlockLength, actingVersion);

            final long sessionId = sessionIdDecoder.sessionId();
            if (sessionId == 0)
            {
                return;
            }

            final int compositeKeyLength = sessionIdDecoder.compositeKeyLength();
            final Object compositeKey = idStrategy.load(
                buffer, filePosition + BLOCK_LENGTH, compositeKeyLength);
            if (compositeKey == null)
            {
                return;
            }

            compositeToSurrogate.put(compositeKey, sessionId);

            filePosition += BLOCK_LENGTH + compositeKeyLength;
        }
    }

    private int validateSectorChecksum(final int position, final int sectorEnd)
    {
        if (position > sectorEnd)
        {
            final int nextSectorEnd = sectorEnd + SECTOR_SIZE;
            final int nextChecksum = nextSectorEnd - CHECKSUM_SIZE;
            crc32.reset();
            byteBuffer.position(sectorEnd).limit(nextChecksum);
            crc32.update(byteBuffer);
            final int calculateChecksum = (int) crc32.getValue();
            final int savedChecksum = buffer.getInt(nextChecksum);
            if (calculateChecksum != savedChecksum)
            {
                throw new FileSystemCorruptionException(
                    String.format(
                        "The session ids file is corrupted between bytes %d and %d, " +
                        "saved checksum is %d, but %d was calculated",
                        sectorEnd,
                        nextSectorEnd,
                        savedChecksum,
                        calculateChecksum));
            }
            return nextSectorEnd;
        }

        return sectorEnd;
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
        // TODO: load up counter from the previous highest seen value
        // TODO: do more efficient checksumming
        final long sessionId = counter++;
        final int compositeKeyLength = idStrategy.save(compositeKey, compositeKeyBuffer, 0);
        if (compositeKeyLength == INSUFFICIENT_SPACE)
        {
            errorHandler.onError(new IllegalStateException(String.format(
                "Unable to save record session id %d for %s, because the buffer is too small",
                sessionId,
                compositeKey)));
        }
        else
        {
            final int nextSectorStart = nextSectorStart(filePosition);
            int checksumOffset = nextSectorStart - CHECKSUM_SIZE;
            final int proposedRecordEnd = filePosition + BLOCK_LENGTH + compositeKeyLength;
            // If the key would span the end of a sector then
            if (proposedRecordEnd > checksumOffset)
            {
                filePosition = nextSectorStart;
                checksumOffset += SECTOR_SIZE;
                crc32.reset();
            }

            sessionIdEncoder
                .wrap(buffer, filePosition)
                .sessionId(sessionId)
                .compositeKeyLength(compositeKeyLength);
            filePosition += BLOCK_LENGTH;

            buffer.putBytes(filePosition, compositeKeyBuffer, 0, compositeKeyLength);
            filePosition += compositeKeyLength;

            byteBuffer.position(nextSectorStart - SECTOR_SIZE).limit(checksumOffset);
            updateChecksum(checksumOffset);
        }

        return sessionId;
    }

    private void updateChecksum(final int checksumOffset)
    {
        crc32.reset();
        crc32.update(byteBuffer);
        final int checksumValue = (int) crc32.getValue();
        buffer.putInt(checksumOffset, checksumValue);
    }

    public void onDisconnect(final long sessionId)
    {
        currentlyAuthenticated.remove(sessionId);
    }

}
