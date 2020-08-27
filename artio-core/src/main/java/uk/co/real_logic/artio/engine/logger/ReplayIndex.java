/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd., Monotonic Ltd.
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
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectCache;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.engine.SequenceNumberExtractor;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.storage.messages.ReplayIndexRecordEncoder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.LongFunction;

import static io.aeron.archive.status.RecordingPos.NULL_RECORDING_ID;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static org.agrona.UnsafeAccess.UNSAFE;
import static uk.co.real_logic.artio.engine.SequenceNumberExtractor.NO_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.engine.logger.ReplayIndexDescriptor.*;
import static uk.co.real_logic.artio.messages.FixMessageDecoder.*;
import static uk.co.real_logic.artio.messages.MessageStatus.OK;

/**
 * Builds an index of a composite key of session id and sequence number for a given stream.
 *
 * Written Positions are stored in a separate file at {@link ReplayIndexDescriptor#replayPositionPath(String, int)}.
 *
 * Buffer Consists of:
 *
 * MessageHeader
 * Head position counter
 * Tail position counter
 * Multiple ReplayIndexRecord entries
 */
public class ReplayIndex implements Index
{
    private final LongFunction<SessionIndex> newSessionIndex = SessionIndex::new;
    private final MessageHeaderDecoder frameHeaderDecoder = new MessageHeaderDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();
    private final ResetSequenceNumberDecoder resetSequenceNumber = new ResetSequenceNumberDecoder();
    private final RedactSequenceUpdateDecoder redactSequenceUpdateDecoder = new RedactSequenceUpdateDecoder();
    private final ReplayIndexRecordEncoder replayIndexRecord = new ReplayIndexRecordEncoder();
    private final MessageHeaderEncoder indexHeaderEncoder = new MessageHeaderEncoder();

    private final IndexedPositionWriter positionWriter;
    private final IndexedPositionReader positionReader;
    private final SequenceNumberExtractor sequenceNumberExtractor;

    private final ILinkSequenceNumberExtractor iLinkSequenceNumberExtractor;

    private final Long2ObjectCache<SessionIndex> fixSessionIdToIndex;

    private final String logFileDir;
    private final int requiredStreamId;
    private final int indexFileSize;
    private final BufferFactory bufferFactory;
    private final AtomicBuffer positionBuffer;
    private final ErrorHandler errorHandler;
    private final RecordingIdLookup recordingIdLookup;

    public ReplayIndex(
        final String logFileDir,
        final int requiredStreamId,
        final int indexFileSize,
        final int cacheNumSets,
        final int cacheSetSize,
        final BufferFactory bufferFactory,
        final AtomicBuffer positionBuffer,
        final ErrorHandler errorHandler,
        final RecordingIdLookup recordingIdLookup,
        final Long2LongHashMap connectionIdToILinkUuid)
    {
        this.logFileDir = logFileDir;
        this.requiredStreamId = requiredStreamId;
        this.indexFileSize = indexFileSize;
        this.bufferFactory = bufferFactory;
        this.positionBuffer = positionBuffer;
        this.errorHandler = errorHandler;
        this.recordingIdLookup = recordingIdLookup;

        iLinkSequenceNumberExtractor = new ILinkSequenceNumberExtractor(
            connectionIdToILinkUuid, errorHandler,
            (sequenceNumber, uuid, messageSize, endPosition, aeronSessionId) ->
                sessionIndex(uuid)
                .onRecord(endPosition, messageSize, sequenceNumber, 0, aeronSessionId, NULL_RECORDING_ID));
        sequenceNumberExtractor = new SequenceNumberExtractor(errorHandler);
        checkIndexFileSize(indexFileSize);
        fixSessionIdToIndex = new Long2ObjectCache<>(cacheNumSets, cacheSetSize, SessionIndex::close);
        final String replayPositionPath = replayPositionPath(logFileDir, requiredStreamId);
        positionWriter = new IndexedPositionWriter(
            positionBuffer, errorHandler, 0, replayPositionPath, recordingIdLookup);
        positionReader = new IndexedPositionReader(positionBuffer);
    }

    private long continuedFixSessionId;
    private int continuedSequenceNumber;
    private int continuedSequenceIndex;

    public void onCatchup(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header,
        final long recordingId)
    {
        onFragment(buffer, offset, length, header, recordingId);
    }

    public void onFragment(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final int streamId = header.streamId();
        if (streamId == requiredStreamId)
        {
            onFragment(buffer, offset, length, header, NULL_RECORDING_ID);
        }
    }

    public void onFragment(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength,
        final Header header,
        final long recordingId)
    {
        final long endPosition = header.position();
        final byte flags = header.flags();
        final int length = BitUtil.align(srcLength, FRAME_ALIGNMENT);

        int offset = srcOffset;
        frameHeaderDecoder.wrap(srcBuffer, offset);
        final int templateId = frameHeaderDecoder.templateId();
        final int blockLength = frameHeaderDecoder.blockLength();
        final int version = frameHeaderDecoder.version();
        offset += frameHeaderDecoder.encodedLength();

        final boolean beginMessage = (flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG;
        if ((flags & UNFRAGMENTED) == UNFRAGMENTED || beginMessage)
        {
            if (templateId == FixMessageEncoder.TEMPLATE_ID)
            {
                messageFrame.wrap(srcBuffer, offset, blockLength, version);
                if (messageFrame.status() == OK)
                {
                    offset += blockLength;
                    if (version >= metaDataSinceVersion())
                    {
                        offset += metaDataHeaderLength() + messageFrame.metaDataLength();
                        messageFrame.skipMetaData();
                    }
                    offset += bodyHeaderLength();

                    final long fixSessionId = messageFrame.session();
                    final int sequenceNumber = sequenceNumberExtractor.extract(
                        srcBuffer, offset, messageFrame.bodyLength());
                    final int sequenceIndex = messageFrame.sequenceIndex();

                    if (sequenceNumber != NO_SEQUENCE_NUMBER)
                    {
                        if (beginMessage)
                        {
                            continuedFixSessionId = fixSessionId;
                            continuedSequenceNumber = sequenceNumber;
                            continuedSequenceIndex = sequenceIndex;
                        }

                        sessionIndex(fixSessionId).onRecord(
                            endPosition, length, sequenceNumber, sequenceIndex, header.sessionId(), recordingId);
                    }
                }
            }
            else if (templateId == ILinkMessageDecoder.TEMPLATE_ID || templateId == ILinkConnectDecoder.TEMPLATE_ID)
            {
                iLinkSequenceNumberExtractor.onFragment(srcBuffer, srcOffset, srcLength, header);
            }
            else if (templateId == ResetSequenceNumberDecoder.TEMPLATE_ID)
            {
                resetSequenceNumber.wrap(srcBuffer, offset, blockLength, version);
                final long fixSessionId = resetSequenceNumber.session();
                onResetSequenceNumber(fixSessionId);
            }
            else if (templateId == RedactSequenceUpdateDecoder.TEMPLATE_ID)
            {
                redactSequenceUpdateDecoder.wrap(srcBuffer, offset, blockLength, version);
                // We only update the replay index in response to a redact if it is used to redact all the sequence
                // numbers within the index
                if (redactSequenceUpdateDecoder.correctSequenceNumber() <= 1)
                {
                    final long fixSessionId = redactSequenceUpdateDecoder.session();
                    onResetSequenceNumber(fixSessionId);
                }
            }
        }
        else
        {
            sessionIndex(continuedFixSessionId).onRecord(
                endPosition, length, continuedSequenceNumber, continuedSequenceIndex, header.sessionId(), recordingId);
        }

        positionWriter.update(header.sessionId(), templateId, endPosition, recordingId);
        positionWriter.updateChecksums();
    }

    private void onResetSequenceNumber(final long fixSessionId)
    {
        final SessionIndex index = fixSessionIdToIndex.remove(fixSessionId);

        if (index != null)
        {
            index.reset();
        }
        else
        {
            // File might be present but not within the cache.
            final File replayIndexFile = replayIndexFile(fixSessionId);
            if (replayIndexFile.exists())
            {
                deleteFile(replayIndexFile);
            }
        }
    }

    private SessionIndex sessionIndex(final long fixSessionId)
    {
        return fixSessionIdToIndex
            .computeIfAbsent(fixSessionId, newSessionIndex);
    }

    public int doWork()
    {
        return positionWriter.checkRecordings();
    }

    public void close()
    {
        positionWriter.close();
        fixSessionIdToIndex.clear();
        IoUtil.unmap(positionBuffer.byteBuffer());
    }

    public void readLastPosition(final IndexedPositionConsumer consumer)
    {
        positionReader.readLastPosition(consumer);
    }

    private final class SessionIndex implements AutoCloseable
    {
        private final ByteBuffer wrappedBuffer;
        private final AtomicBuffer buffer;
        private final int recordCapacity;
        private final File replayIndexFile;

        SessionIndex(final long fixSessionId)
        {
            replayIndexFile = replayIndexFile(fixSessionId);
            final boolean exists = replayIndexFile.exists();
            this.wrappedBuffer = bufferFactory.map(replayIndexFile, indexFileSize);
            this.buffer = new UnsafeBuffer(wrappedBuffer);

            recordCapacity = recordCapacity(buffer.capacity());
            if (!exists)
            {
                indexHeaderEncoder
                    .wrap(buffer, 0)
                    .blockLength(replayIndexRecord.sbeBlockLength())
                    .templateId(replayIndexRecord.sbeTemplateId())
                    .schemaId(replayIndexRecord.sbeSchemaId())
                    .version(replayIndexRecord.sbeSchemaVersion());
            }
            else
            {
                // Reset the positions in order to avoid wraps at the start.
                final long resetPosition = beginChange(buffer);
                endChangeOrdered(buffer, resetPosition);
            }
        }

        void onRecord(
            final long endPosition,
            final int length,
            final int sequenceNumber,
            final int sequenceIndex,
            final int aeronSessionId,
            final long knownRecordingId)
        {
            final long beginChangePosition = beginChange(buffer);
            final long changePosition = beginChangePosition + RECORD_LENGTH;
            final long recordingId = knownRecordingId ==
                NULL_RECORDING_ID ? recordingIdLookup.getRecordingId(aeronSessionId) : knownRecordingId;
            final long beginPosition = endPosition - length;

            beginChangeOrdered(buffer, changePosition);
            UNSAFE.storeFence();

            final int offset = offset(beginChangePosition, recordCapacity);

            replayIndexRecord
                .wrap(buffer, offset)
                .position(beginPosition)
                .sequenceNumber(sequenceNumber)
                .sequenceIndex(sequenceIndex)
                .recordingId(recordingId)
                .length(length);

            endChangeOrdered(buffer, changePosition);
        }

        void reset()
        {
            close();
            deleteFile(replayIndexFile);
        }

        public void close()
        {
            IoUtil.unmap(wrappedBuffer);
        }
    }

    private File replayIndexFile(final long fixSessionId)
    {
        return ReplayIndexDescriptor.replayIndexFile(logFileDir, fixSessionId, requiredStreamId);
    }

    private void deleteFile(final File replayIndexFile)
    {
        if (!replayIndexFile.delete())
        {
            errorHandler.onError(new IOException("Unable to delete replay index file: " + replayIndexFile));
        }
    }
}
