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

import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.engine.MappedFile;
import uk.co.real_logic.fix_gateway.engine.SectorFramer;
import uk.co.real_logic.fix_gateway.engine.logger.LoggerUtil;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.storage.messages.SessionIdDecoder;
import uk.co.real_logic.fix_gateway.storage.messages.SessionIdEncoder;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.zip.CRC32;

import static uk.co.real_logic.fix_gateway.engine.SectorFramer.*;
import static uk.co.real_logic.fix_gateway.session.SessionIdStrategy.INSUFFICIENT_SPACE;
import static uk.co.real_logic.fix_gateway.storage.messages.SessionIdEncoder.BLOCK_LENGTH;

/**
 * Identifies which sessions are currently authenticated.
 *
 * The session ids table is saved into a file. Records are written out using the {@link SessionIdEncoder}
 * and aren't allowed to span sectors. Each sector has a CRC32 checksum and each checksum is updated after writing
 * each session id record.
 */
public class SessionContexts
{
    public static final long MISSING_SESSION_ID = -2;

    static final SessionContext DUPLICATE_SESSION = new SessionContext(-3, -3, null, OUT_OF_SPACE);
    static final SessionContext UNKNOWN_SESSION = new SessionContext(
        Session.UNKNOWN, (int)Session.UNKNOWN, null, OUT_OF_SPACE);
    static final long LOWEST_VALID_SESSION_ID = 1L;

    private static final int HEADER_SIZE = MessageHeaderDecoder.ENCODED_LENGTH;

    private static final int ENCODING_BUFFER_SIZE = SECTOR_SIZE - CHECKSUM_SIZE;
    private final UnsafeBuffer compositeKeyBuffer = new UnsafeBuffer(new byte[ENCODING_BUFFER_SIZE]);

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final SessionIdEncoder sessionIdEncoder = new SessionIdEncoder();
    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final LogonDecoder logonDecoder = new LogonDecoder();
    private final int actingBlockLength = sessionIdEncoder.sbeBlockLength();
    private final int actingVersion = sessionIdEncoder.sbeSchemaVersion();

    private final Function<CompositeKey, SessionContext> onNewLogonFunc = this::onNewLogon;
    private final LongHashSet currentlyAuthenticatedSessionIds = new LongHashSet(MISSING_SESSION_ID);
    private final LongHashSet recordedSessions = new LongHashSet(MISSING_SESSION_ID);
    private final Map<CompositeKey, SessionContext> compositeToContext = new HashMap<>();

    private final CRC32 crc32 = new CRC32();
    private final SectorFramer sectorFramer;
    private final ByteBuffer byteBuffer;

    private final AtomicBuffer buffer;
    private final SessionIdStrategy idStrategy;
    private final ErrorHandler errorHandler;
    private final MappedFile mappedFile;

    private int filePosition;
    private long counter = LOWEST_VALID_SESSION_ID;

    public SessionContexts(
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
            final int sequenceIndex = sessionIdDecoder.sequenceIndex();

            final int compositeKeyLength = sessionIdDecoder.compositeKeyLength();
            final CompositeKey compositeKey = idStrategy.load(
                buffer, filePosition + BLOCK_LENGTH, compositeKeyLength);
            if (compositeKey == null)
            {
                return;
            }

            compositeToContext.put(compositeKey, new SessionContext(sessionId, sequenceIndex, this, filePosition));
            recordedSessions.add(sessionId);
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
            actingBlockLength,
            errorHandler))
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
            final int calculatedChecksum = (int)crc32.getValue();
            final int savedChecksum = buffer.getInt(nextChecksum);
            validateCheckSum(
                "session ids", sectorEnd, nextSectorEnd, savedChecksum, calculatedChecksum, errorHandler);
            return nextSectorEnd;
        }

        return sectorEnd;
    }

    public SessionContext onLogon(final CompositeKey compositeKey)
    {
        final SessionContext sessionContext = compositeToContext.computeIfAbsent(compositeKey, onNewLogonFunc);

        if (!currentlyAuthenticatedSessionIds.add(sessionContext.sessionId()))
        {
            return DUPLICATE_SESSION;
        }

        return sessionContext;
    }

    private SessionContext onNewLogon(final CompositeKey compositeKey)
    {
        final long sessionId = counter++;
        return assignSessionId(compositeKey, sessionId, SessionContext.UNKNOWN_SEQUENCE_INDEX);
    }

    private SessionContext assignSessionId(final CompositeKey compositeKey,
        final long sessionId,
        final int sequenceIndex)
    {
        int keyPosition = OUT_OF_SPACE;
        final int compositeKeyLength = idStrategy.save(compositeKey, compositeKeyBuffer, 0);
        if (compositeKeyLength == INSUFFICIENT_SPACE)
        {
            errorHandler.onError(new IllegalStateException(String.format(
                "Unable to save record session id %d for %s, because the buffer is too small",
                sessionId,
                compositeKey)));
            return new SessionContext(sessionId, sequenceIndex, this, OUT_OF_SPACE);
        }
        else
        {
            if (filePosition != OUT_OF_SPACE)
            {
                filePosition = sectorFramer.claim(filePosition, BLOCK_LENGTH + compositeKeyLength);
                keyPosition = filePosition;
                if (filePosition == OUT_OF_SPACE)
                {
                    errorHandler.onError(new IllegalStateException(
                        "Run out of space when storing: " + compositeKey));
                }
                else
                {
                    sessionIdEncoder
                        .wrap(buffer, filePosition)
                        .sessionId(sessionId)
                        .sequenceIndex(sequenceIndex)
                        .compositeKeyLength(compositeKeyLength);
                    filePosition += BLOCK_LENGTH;

                    buffer.putBytes(filePosition, compositeKeyBuffer, 0, compositeKeyLength);
                    filePosition += compositeKeyLength;

                    updateChecksum(sectorFramer.sectorStart(), sectorFramer.checksumOffset());
                    mappedFile.force();
                }
            }

            return new SessionContext(sessionId, sequenceIndex, this, keyPosition);
        }
    }

    void sequenceReset(final long sessionId)
    {
        compositeToContext
            .values()
            .stream()
            .filter(context -> context.sessionId() == sessionId)
            .forEach(SessionContext::onSequenceReset);
    }

    // TODO: optimisation, more efficient checksumming, only checksum new data
    private void updateChecksum(final int start, final int checksumOffset)
    {
        final int endOfData = checksumOffset;
        byteBuffer.clear().position(start).limit(endOfData);
        crc32.reset();
        crc32.update(byteBuffer);
        final int checksumValue = (int)crc32.getValue();
        buffer.putInt(checksumOffset, checksumValue);
    }

    public void onDisconnect(final long sessionId)
    {
        currentlyAuthenticatedSessionIds.remove(sessionId);
    }

    public void reset(final File backupLocation)
    {
        if (!currentlyAuthenticatedSessionIds.isEmpty())
        {
            throw new IllegalStateException(
                "There are currently authenticated sessions: " + currentlyAuthenticatedSessionIds);
        }

        counter = LOWEST_VALID_SESSION_ID;
        currentlyAuthenticatedSessionIds.clear();
        compositeToContext.clear();

        if (backupLocation != null)
        {
            mappedFile.transferTo(backupLocation);
        }

        buffer.setMemory(0, buffer.capacity(), (byte)0);
        initialiseBuffer();
    }

    void onSentFollowerMessage(
        final long sessionId,
        final int sequenceIndex,
        final int messageType,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        if (messageType == LogonDecoder.MESSAGE_TYPE && recordedSessions.add(sessionId))
        {
            // Ensure no future collision if you take over as leader of the cluster.
            counter = sessionId + 1;

            asciiBuffer.wrap(buffer);
            logonDecoder.decode(asciiBuffer, offset, length);

            // We use the initiator logon variant as we are reading a sent message.
            final HeaderDecoder header = logonDecoder.header();
            onSentFollowerLogon(header, sessionId, sequenceIndex);
        }
    }

    void onSentFollowerLogon(final HeaderDecoder header, final long sessionId, final int sequenceIndex)
    {
        final CompositeKey compositeKey = idStrategy.onInitiateLogon(
            header.senderCompIDAsString(),
            header.senderSubIDAsString(),
            header.senderLocationIDAsString(),
            header.targetCompIDAsString(),
            header.targetSubIDAsString(),
            header.targetLocationIDAsString());

        final SessionContext sessionContext = assignSessionId(compositeKey, sessionId, sequenceIndex);
        compositeToContext.put(compositeKey, sessionContext);
    }

    void updateSequenceIndex(final int filePosition, final int sequenceIndex)
    {
        sessionIdEncoder
            .wrap(buffer, filePosition)
            .sequenceIndex(sequenceIndex);

        final int start = nextSectorStart(filePosition) - SECTOR_SIZE;
        final int checksumOffset = start + SECTOR_DATA_LENGTH;
        updateChecksum(start, checksumOffset);

        mappedFile.force();
    }
}
