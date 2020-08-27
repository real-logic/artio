/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import org.agrona.ErrorHandler;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.artio.engine.ChecksumFramer;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.storage.messages.IndexedPositionDecoder;
import uk.co.real_logic.artio.storage.messages.IndexedPositionEncoder;

import static io.aeron.archive.status.RecordingPos.NULL_RECORDING_ID;
import static uk.co.real_logic.artio.engine.SectorFramer.OUT_OF_SPACE;

/**
 * Writes out a log of the stream positions that we have indexed up to.
 * Not thread safe, but writes to a thread safe buffer.
 */
class IndexedPositionWriter
{
    static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;
    static final int RECORD_LENGTH = IndexedPositionEncoder.BLOCK_LENGTH;
    static final int POSITION_OFFSET = IndexedPositionEncoder.positionEncodingOffset();

    private static final int MISSING_RECORD = -1;

    private final IndexedPositionEncoder encoder = new IndexedPositionEncoder();
    private final int actingBlockLength = encoder.sbeBlockLength();
    private final int actingVersion = encoder.sbeSchemaVersion();
    private final IndexedPositionDecoder decoder = new IndexedPositionDecoder();
    private final Long2LongHashMap recordOffsets = new Long2LongHashMap(MISSING_RECORD);
    private final AtomicBuffer buffer;
    private final ErrorHandler errorHandler;
    private final RecordingIdLookup recordingIdLookup;
    private final ChecksumFramer checksumFramer;
    private final Long2LongHashMap recheckSessions = new Long2LongHashMap(MISSING_RECORD);

    IndexedPositionWriter(
        final AtomicBuffer buffer,
        final ErrorHandler errorHandler,
        final int errorReportingOffset,
        final String fileName,
        final RecordingIdLookup recordingIdLookup)
    {
        this.buffer = buffer;
        this.errorHandler = errorHandler;
        this.recordingIdLookup = recordingIdLookup;
        checksumFramer = new ChecksumFramer(
            buffer, buffer.capacity(), errorHandler, errorReportingOffset, fileName);
        setupHeader();
        initialiseOffsets();
    }

    private void initialiseOffsets()
    {
        int offset = HEADER_LENGTH;
        while (true)
        {
            offset = checksumFramer.claim(offset, RECORD_LENGTH);
            if (offset == OUT_OF_SPACE)
            {
                return;
            }

            decoder.wrap(buffer, offset, actingBlockLength, actingVersion);
            if (decoder.position() != 0)
            {
                recordOffsets.put(decoder.recordingId(), offset);
            }
            offset += RECORD_LENGTH;
        }
    }

    private void setupHeader()
    {
        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

        messageHeaderDecoder.wrap(buffer, 0);
        if (messageHeaderDecoder.blockLength() == 0)
        {
            messageHeaderEncoder
                .wrap(buffer, 0)
                .templateId(encoder.sbeTemplateId())
                .schemaId(encoder.sbeSchemaId())
                .blockLength(actingBlockLength)
                .version(actingVersion);

            checksumFramer.updateChecksums();
        }
        else
        {
            checksumFramer.validateCheckSums();
        }
    }

    void indexedUpTo(final int aeronSessionId, final long recordingId, final long position)
    {
        final Long2LongHashMap recordOffsets = this.recordOffsets;

        int offset = (int)recordOffsets.get(recordingId);
        if (offset == MISSING_RECORD)
        {
            final IndexedPositionDecoder decoder = this.decoder;
            final int actingBlockLength = this.actingBlockLength;
            final int actingVersion = this.actingVersion;
            final AtomicBuffer buffer = this.buffer;

            offset = HEADER_LENGTH;
            while (true)
            {
                offset = checksumFramer.claim(offset, RECORD_LENGTH);
                if (offset == OUT_OF_SPACE)
                {
                    errorHandler.onError(new IllegalStateException(String.format(
                        "Unable to record new session (%d), indexed position buffer full",
                        aeronSessionId)));
                    return;
                }

                decoder.wrap(buffer, offset, actingBlockLength, actingVersion);
                if (decoder.position() == 0)
                {
                    encoder
                        .wrap(buffer, offset)
                        .sessionId(aeronSessionId)
                        .recordingId(recordingId);

                    recordOffsets.put(recordingId, offset);
                    putPosition(position, buffer, offset);
                    return;
                }

                offset += RECORD_LENGTH;
            }
        }
        else
        {
            putPosition(position, buffer, offset);
        }
    }

    void close()
    {
        updateChecksums();
    }

    void updateChecksums()
    {
        checksumFramer.updateChecksums();
    }

    AtomicBuffer buffer()
    {
        return buffer;
    }

    private void putPosition(final long position, final AtomicBuffer buffer, final int offset)
    {
        buffer.putLongVolatile(offset + POSITION_OFFSET, position);
    }

    public void trackPosition(final int aeronSessionId, final long endPosition)
    {
        recheckSessions.remove(aeronSessionId);
        if (!checkPosition(aeronSessionId, endPosition))
        {
            recheckSessions.put(aeronSessionId, endPosition);
        }
    }

    private boolean checkPosition(final int aeronSessionId, final long endPosition)
    {
        final long recordingId = recordingIdLookup.findRecordingId(aeronSessionId);
        if (recordingId != NULL_RECORDING_ID)
        {
            indexedUpTo(aeronSessionId, recordingId, endPosition);
            return true;
        }
        return false;
    }

    public int checkRecordings()
    {
        int work = 0;
        final Long2LongHashMap.EntryIterator it = recheckSessions.entrySet().iterator();
        while (it.hasNext())
        {
            it.next();
            final int aeronSessionId = (int)it.getLongKey();
            final long endPosition = it.getLongValue();
            if (checkPosition(aeronSessionId, endPosition))
            {
                it.remove();
                work++;
            }
        }
        return work;
    }

    public void update(
        final int aeronSessionId, final int templateId, final long endPosition, final long knownRecordingId)
    {
        long recordingId = knownRecordingId;
        if (recordingId == NULL_RECORDING_ID)
        {
            switch (templateId)
            {
                // May not have setup the recording id when these messages come in.
                case LibraryConnectDecoder.TEMPLATE_ID:
                case ApplicationHeartbeatDecoder.TEMPLATE_ID:
                    trackPosition(aeronSessionId, endPosition);
                    return;

                // Outbound stream, so don't need to update the indexed position.
                case ValidResendRequestDecoder.TEMPLATE_ID:
                case RedactSequenceUpdateDecoder.TEMPLATE_ID:
                    return;
            }
            recordingId = recordingIdLookup.getRecordingId(aeronSessionId);
        }

        // For other messages block until the recording id is setup.
        indexedUpTo(aeronSessionId, recordingId, endPosition);
    }
}
