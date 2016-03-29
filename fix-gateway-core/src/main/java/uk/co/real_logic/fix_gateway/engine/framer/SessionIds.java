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
package uk.co.real_logic.fix_gateway.engine.framer;

import org.agrona.ErrorHandler;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.engine.SectorFramer;
import uk.co.real_logic.fix_gateway.engine.MappedFile;
import uk.co.real_logic.fix_gateway.engine.logger.LoggerUtil;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.messages.SessionIdDecoder;
import uk.co.real_logic.fix_gateway.messages.SessionIdEncoder;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.zip.CRC32;

import static uk.co.real_logic.fix_gateway.engine.SectorFramer.*;
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
    public static final long LOWEST_VALID_SESSION_ID = 1L;

    private static final int HEADER_SIZE = MessageHeaderDecoder.ENCODED_LENGTH;

    private static final int ENCODING_BUFFER_SIZE = SECTOR_SIZE - CHECKSUM_SIZE;
    private final UnsafeBuffer compositeKeyBuffer = new UnsafeBuffer(new byte[ENCODING_BUFFER_SIZE]);

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final SessionIdEncoder sessionIdEncoder = new SessionIdEncoder();
    private final int actingBlockLength = sessionIdEncoder.sbeBlockLength();
    private final int actingVersion = sessionIdEncoder.sbeSchemaVersion();

    private final Function<CompositeKey, Long> onLogonFunc = this::onNewLogon;
    private final LongHashSet currentlyAuthenticated = new LongHashSet(MISSING);
    private final Map<CompositeKey, Long> compositeToSurrogate = new HashMap<>();

    private final CRC32 crc32 = new CRC32();
    private final SectorFramer sectorFramer;
    private final ByteBuffer byteBuffer;

    private final AtomicBuffer buffer;
    private final SessionIdStrategy idStrategy;
    private final ErrorHandler errorHandler;
    private final MappedFile mappedFile;

    private int filePosition;
    private long counter = LOWEST_VALID_SESSION_ID;

    public SessionIds(
        final MappedFile mappedFile, final SessionIdStrategy idStrategy, final ErrorHandler errorHandler)
    {
        this.mappedFile = mappedFile;
        this.buffer = mappedFile.buffer();
        this.byteBuffer = this.buffer.byteBuffer();
        sectorFramer = new SectorFramer(buffer.capacity());
        this.idStrategy = idStrategy;
        this.errorHandler = errorHandler;
        loadBuffer();
    }

    private void loadBuffer()
    {
        checkByteBuffer();
        initialiseBuffer();

        final SessionIdDecoder sessionIdDecoder = new SessionIdDecoder();

        int sectorEnd = 0;
        filePosition = HEADER_SIZE;
        final int lastRecordStart = buffer.capacity() - BLOCK_LENGTH;
        while (filePosition < lastRecordStart)
        {
            sectorEnd = validateSectorChecksum(filePosition, sectorEnd);
            long sessionId = wrap(sessionIdDecoder, filePosition);
            if (sessionId == 0)
            {
                final int nextSectorPeekPosition = sectorEnd;
                if (nextSectorPeekPosition > lastRecordStart)
                {
                    return;
                }

                sessionId = wrap(sessionIdDecoder, nextSectorPeekPosition);
                if (sessionId == 0)
                {
                    return;
                }
                else
                {
                    filePosition = nextSectorPeekPosition;
                }
            }

            final int compositeKeyLength = sessionIdDecoder.compositeKeyLength();
            final CompositeKey compositeKey = idStrategy.load(
                buffer, filePosition + BLOCK_LENGTH, compositeKeyLength);
            if (compositeKey == null)
            {
                return;
            }

            compositeToSurrogate.put(compositeKey, sessionId);
            counter = Math.max(counter, sessionId + 1);

            filePosition += BLOCK_LENGTH + compositeKeyLength;
        }
    }

    private long wrap(final SessionIdDecoder sessionIdDecoder, final int nextSectorPeekPosition)
    {
        sessionIdDecoder.wrap(buffer, nextSectorPeekPosition, actingBlockLength, actingVersion);
        return sessionIdDecoder.sessionId();
    }

    private void checkByteBuffer()
    {
        if (byteBuffer == null)
        {
            throw new IllegalStateException("Must use atomic buffer backed by a byte buffer");
        }
    }

    private void initialiseBuffer()
    {
        if (LoggerUtil.initialiseBuffer(
            buffer,
            headerEncoder,
            headerDecoder,
            sessionIdEncoder.sbeSchemaId(),
            sessionIdEncoder.sbeTemplateId(),
            actingVersion,
            actingBlockLength))
        {
            updateChecksum(0, FIRST_CHECKSUM_LOCATION);
            mappedFile.force();
        }
    }

    private int validateSectorChecksum(final int position, final int sectorEnd)
    {
        if (position > sectorEnd)
        {
            final int nextSectorEnd = sectorEnd + SECTOR_SIZE;
            final int nextChecksum = nextSectorEnd - CHECKSUM_SIZE;
            crc32.reset();
            byteBuffer.clear().position(sectorEnd).limit(nextChecksum);
            crc32.update(byteBuffer);
            final int calculatedChecksum = (int) crc32.getValue();
            final int savedChecksum = buffer.getInt(nextChecksum);
            validateCheckSum(sectorEnd, nextSectorEnd, calculatedChecksum, savedChecksum, "session ids");
            return nextSectorEnd;
        }

        return sectorEnd;
    }

    public long onLogon(final CompositeKey compositeKey)
    {
        final Long sessionId = compositeToSurrogate.computeIfAbsent(compositeKey, onLogonFunc);

        if (!currentlyAuthenticated.add(sessionId))
        {
            return DUPLICATE_SESSION;
        }

        return sessionId;
    }

    private long onNewLogon(final CompositeKey compositeKey)
    {
        final long sessionId = counter++;
        final int compositeKeyLength = idStrategy.save(compositeKey, compositeKeyBuffer, 0);
        if (compositeKeyLength == INSUFFICIENT_SPACE)
        {
            errorHandler.onError(new IllegalStateException(String.format(
                "Unable to save record session id %d for %s, because the buffer is too small",
                sessionId,
                compositeKey)));
        }
        else if (filePosition != OUT_OF_SPACE)
        {
            filePosition = sectorFramer.claim(filePosition, BLOCK_LENGTH + compositeKeyLength);
            if (filePosition == OUT_OF_SPACE)
            {
                errorHandler.onError(new IllegalStateException(
                    "Run out of space when storing: " + compositeKey));
                return sessionId;
            }

            sessionIdEncoder
                .wrap(buffer, filePosition)
                .sessionId(sessionId)
                .compositeKeyLength(compositeKeyLength);
            filePosition += BLOCK_LENGTH;

            buffer.putBytes(filePosition, compositeKeyBuffer, 0, compositeKeyLength);
            filePosition += compositeKeyLength;

            updateChecksum(sectorFramer.sectorStart(), sectorFramer.checksumOffset());
            mappedFile.force();
        }

        return sessionId;
    }

    // TODO: optimisation, more efficient checksumming, only checksum new data
    private void updateChecksum(final int start, final int checksumOffset)
    {
        final int endOfData = checksumOffset;
        byteBuffer.position(start).limit(endOfData);
        crc32.reset();
        crc32.update(byteBuffer);
        final int checksumValue = (int) crc32.getValue();
        buffer.putInt(checksumOffset, checksumValue);
    }

    public void onDisconnect(final long sessionId)
    {
        currentlyAuthenticated.remove(sessionId);
    }

    public void reset(final File backupLocation)
    {
        if (!currentlyAuthenticated.isEmpty())
        {
            throw new IllegalStateException("There are currently authenticated sessions: " + currentlyAuthenticated);
        }

        counter = LOWEST_VALID_SESSION_ID;
        currentlyAuthenticated.clear();
        compositeToSurrogate.clear();

        mappedFile.transferTo(backupLocation);

        buffer.setMemory(0, buffer.capacity(), (byte) 0);
        initialiseBuffer();
    }
}
