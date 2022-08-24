package uk.co.real_logic.artio.engine.logger;

import org.agrona.LangUtil;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.storage.messages.ReplayIndexRecordDecoder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static io.aeron.Aeron.NULL_VALUE;
import static uk.co.real_logic.artio.builder.Encoder.BITS_IN_INT;
import static uk.co.real_logic.artio.engine.logger.ReplayIndexDescriptor.*;
import static uk.co.real_logic.artio.engine.logger.ReplayQuery.trueBeginPosition;

/**
 * Utility for extracting information from replay index file. Mostly used for debugging Artio state.
 * Experimental: API subject to change
 */
public final class ReplayIndexExtractor
{
    public interface ReplayIndexHandler
    {
        void onEntry(ReplayIndexRecordDecoder indexRecord);

        void onLapped();
    }

    public static class StartPositionExtractor implements ReplayIndexExtractor.ReplayIndexHandler
    {
        private final StartPositionQuery startPositionQuery = new StartPositionQuery();

        public void onEntry(final ReplayIndexRecordDecoder indexRecord)
        {
            final long beginPosition = indexRecord.position();
            final int sequenceIndex = indexRecord.sequenceIndex();
            final long recordingId = indexRecord.recordingId();
            final int sequenceNumber = indexRecord.sequenceNumber();

            startPositionQuery.updateStartPosition(sequenceNumber, sequenceIndex, recordingId, beginPosition);
        }

        public void onLapped()
        {
            System.err.println("Error: lapped by writer currently updating the file");
        }

        public Long2ObjectHashMap<PrunePosition> recordingIdToStartPosition()
        {
            return startPositionQuery.recordingIdToStartPosition();
        }

        public int highestSequenceIndex()
        {
            return startPositionQuery.highestSequenceIndex();
        }
    }

    public static class SequencePosition
    {
        private final long sequenceIndex;
        private final long position;

        public SequencePosition(final long sequenceIndex, final long position)
        {
            this.sequenceIndex = sequenceIndex;
            this.position = position;
        }

        public long position()
        {
            return position;
        }

        public long sequenceIndex()
        {
            return sequenceIndex;
        }

        public String toString()
        {
            return "SequencePosition{" +
                "sequenceIndex=" + sequenceIndex +
                ", position=" + position +
                '}';
        }
    }

    public static class BoundaryPositionExtractor implements ReplayIndexExtractor.ReplayIndexHandler
    {
        private final Long2LongHashMap recordingIdToPosition = new Long2LongHashMap(NULL_VALUE);
        private final Long2ObjectHashMap<Long2LongHashMap> recordingIdToSequenceIndexToPosition =
            new Long2ObjectHashMap<>();

        private final boolean min;

        public BoundaryPositionExtractor(final boolean min)
        {
            this.min = min;
        }

        public void onEntry(final ReplayIndexRecordDecoder indexRecord)
        {
            final long beginPosition = trueBeginPosition(indexRecord.position());
            final int sequenceIndex = indexRecord.sequenceIndex();
            final long recordingId = indexRecord.recordingId();

            boundaryUpdate(recordingIdToPosition, beginPosition, recordingId, min);

            final Long2LongHashMap sequenceIndexToPosition = recordingIdToSequenceIndexToPosition.computeIfAbsent(
                recordingId, k -> new Long2LongHashMap(NULL_VALUE));

            boundaryUpdate(sequenceIndexToPosition, beginPosition, sequenceIndex, true);
        }

        private void boundaryUpdate(
            final Long2LongHashMap keyToPosition, final long beginPosition, final long key, final boolean min)
        {
            final long oldPosition = keyToPosition.get(key);
            if (beyondBounary(oldPosition, beginPosition, min))
            {
                keyToPosition.put(key, beginPosition);
            }
        }

        private boolean beyondBounary(final long oldPosition, final long beginPosition, final boolean min)
        {
            if (oldPosition == NULL_VALUE)
            {
                return true;
            }

            if (min)
            {
                return beginPosition < oldPosition;
            }
            else
            {
                return beginPosition > oldPosition;
            }
        }

        public void onLapped()
        {
            System.err.println("Error: lapped by writer currently updating the file");
        }

        public Long2LongHashMap recordingIdToPosition()
        {
            return recordingIdToPosition;
        }

        public Long2ObjectHashMap<Long2LongHashMap> recordingIdToSequenceIndexToPosition()
        {
            return recordingIdToSequenceIndexToPosition;
        }

        public void findInconsistentSequenceIndexPositions()
        {
            recordingIdToSequenceIndexToPosition.forEach((recordingId, sequenceIndexToPosition) ->
            {
                final List<SequencePosition> sequencePositions = sequenceIndexToPosition
                    .entrySet()
                    .stream()
                    .map(e -> new SequencePosition(e.getKey(), e.getValue()))
                    .sorted(Comparator.comparingLong(SequencePosition::position))
                    .collect(Collectors.toList());

                sequenceIndexToPosition.forEach((sequenceIndex, position) ->
                {
                    sequencePositions
                        .stream()
                        .filter(rp -> rp.position < position && rp.sequenceIndex > sequenceIndex)
                        .findFirst()
                        .ifPresent(sp ->
                        System.out.println("Found suppressor for " + sequenceIndex + " @ " + position + ": " +
                        sp.sequenceIndex + " @ " + sp.position));
                });
            });
        }
    }

    public static class PrintError implements ReplayIndexExtractor.ReplayIndexHandler
    {
        private final BufferedWriter out;

        public PrintError(final BufferedWriter out) throws IOException
        {
            this.out = out;
            out.write("beginPosition,sequenceIndex,sequenceNumber,recordingId,readLength\n");
        }

        public void onEntry(final ReplayIndexRecordDecoder indexRecord)
        {
            final long beginPosition = indexRecord.position();
            final int sequenceIndex = indexRecord.sequenceIndex();
            final int sequenceNumber = indexRecord.sequenceNumber();
            final long recordingId = indexRecord.recordingId();
            final int readLength = indexRecord.length();

            try
            {
                out.write(
                    beginPosition + "," +
                    sequenceIndex + "," +
                    sequenceNumber + "," +
                    recordingId + "," +
                    readLength + "\n");
            }
            catch (final IOException e)
            {
                LangUtil.rethrowUnchecked(e);
            }
        }

        public void onLapped()
        {
            System.err.println("Error: lapped by writer currently updating the file");
        }
    }

    public static class ValidationError
    {
        private final int sequenceIndex;
        private final int sequenceNumber;
        private final long position;
        private final int length;
        private final long endPosition;

        public ValidationError(
            final int sequenceIndex,
            final int sequenceNumber,
            final long position,
            final int length,
            final long endPosition)
        {
            this.sequenceIndex = sequenceIndex;
            this.sequenceNumber = sequenceNumber;
            this.position = position;
            this.length = length;
            this.endPosition = endPosition;
        }

        public int sequenceIndex()
        {
            return sequenceIndex;
        }

        public int sequenceNumber()
        {
            return sequenceNumber;
        }

        public long position()
        {
            return position;
        }

        public long endPosition()
        {
            return endPosition;
        }

        public String toString()
        {
            return "ValidationError{" +
                "sequenceIndex=" + sequenceIndex +
                ", sequenceNumber=" + sequenceNumber +
                ", position=" + position +
                ", length=" + length +
                ", endPosition=" + endPosition +
                '}';
        }
    }

    // Validates that there are no non-contiguous duplicate entries
    public static class ReplayIndexValidator implements ReplayIndexHandler
    {
        private static final long MISSING = Long.MIN_VALUE;

        private final Long2LongHashMap sequenceIdToEndPosition = new Long2LongHashMap(MISSING);
        private final List<ValidationError> errors = new ArrayList<>();

        public void onEntry(final ReplayIndexRecordDecoder indexRecord)
        {
            final int sequenceIndex = indexRecord.sequenceIndex();
            final int sequenceNumber = indexRecord.sequenceNumber();
            final long position = indexRecord.position();
            final int length = indexRecord.length();

            final long sequenceId = sequenceIndex | ((long)sequenceNumber) << BITS_IN_INT;
            final long endPosition = position + length;

            final long oldEndPosition = sequenceIdToEndPosition.put(sequenceId, endPosition);
            if (oldEndPosition != MISSING)
            {
                if (oldEndPosition != position)
                {
                    errors.add(new ValidationError(
                        sequenceIndex,
                        sequenceNumber,
                        position,
                        length,
                        endPosition));
                }
            }
        }

        public List<ValidationError> errors()
        {
            return errors;
        }

        public void onLapped()
        {
            sequenceIdToEndPosition.clear();
        }
    }

    public static void extract(
        final EngineConfiguration configuration,
        final long fixSessionId,
        final boolean inbound,
        final ReplayIndexHandler handler)
    {
        final int streamId = inbound ? configuration.inboundLibraryStream() : configuration.outboundLibraryStream();
        final String logFileDir = configuration.logFileDir();
        final File file = replayIndexHeaderFile(logFileDir, fixSessionId, streamId);
        if (file.exists())
        {
            extract(file, configuration.replayIndexFileRecordCapacity(),
                configuration.replayIndexSegmentRecordCapacity(), fixSessionId, streamId, logFileDir, handler);
        }
    }

    public static void extract(
        final File headerFile,
        final int indexFileCapacity,
        final int indexSegmentCapacity,
        final long fixSessionId,
        final int streamId,
        final String logFileDir,
        final ReplayIndexHandler handler)
    {
        final long indexFileSize = capacityToBytes(indexFileCapacity);
        final int segmentSize = capacityToBytesInt(indexSegmentCapacity);
        final int segmentCount = segmentCount(indexFileCapacity, indexSegmentCapacity);
        final UnsafeBuffer[] segmentBuffers = new UnsafeBuffer[segmentCount];
        final int segmentSizeBitShift = Long.numberOfTrailingZeros(segmentSize);

        final UnsafeBuffer headerBuffer = new UnsafeBuffer(LoggerUtil.mapExistingFile(headerFile));
        try
        {
            final MessageHeaderDecoder messageFrameHeader = new MessageHeaderDecoder();
            final ReplayIndexRecordDecoder indexRecord = new ReplayIndexRecordDecoder();

            messageFrameHeader.wrap(headerBuffer, 0);
            final int actingBlockLength = messageFrameHeader.blockLength();
            final int actingVersion = messageFrameHeader.version();

            long iteratorPosition = beginChangeVolatile(headerBuffer);
            long stopIteratingPosition = iteratorPosition + indexFileSize;

            while (iteratorPosition < stopIteratingPosition)
            {
                final long changePosition = endChangeVolatile(headerBuffer);

                if (changePosition > iteratorPosition &&
                    (iteratorPosition + indexFileSize) <= beginChangeVolatile(headerBuffer))
                {
                    handler.onLapped();
                    iteratorPosition = changePosition;
                    stopIteratingPosition = iteratorPosition + indexFileSize;
                }

                final UnsafeBuffer segmentBuffer = segmentBuffer(
                    iteratorPosition, segmentSizeBitShift, segmentBuffers, indexFileSize,
                    fixSessionId, streamId, logFileDir);
                final int offset = offsetInSegment(iteratorPosition, segmentSize);
                indexRecord.wrap(segmentBuffer, offset, actingBlockLength, actingVersion);
                final long beginPosition = indexRecord.position();

                if (beginPosition == 0)
                {
                    break;
                }

                handler.onEntry(indexRecord);

                iteratorPosition += RECORD_LENGTH;
            }
        }
        finally
        {
            ReplayIndexDescriptor.unmapBuffers(headerBuffer, segmentBuffers);
        }
    }

    private static UnsafeBuffer segmentBuffer(
        final long position,
        final int segmentSizeBitShift,
        final UnsafeBuffer[] segmentBuffers,
        final long indexFileSize,
        final long fixSessionId,
        final int streamId, final String logFileDir)
    {
        final int segmentIndex = ReplayIndexDescriptor.segmentIndex(position, segmentSizeBitShift, indexFileSize);
        UnsafeBuffer segmentBuffer = segmentBuffers[segmentIndex];
        if (segmentBuffer == null)
        {
            final File file = replayIndexSegmentFile(logFileDir, fixSessionId, streamId, segmentIndex);
            segmentBuffer = new UnsafeBuffer(LoggerUtil.mapExistingFile(file));
            segmentBuffers[segmentIndex] = segmentBuffer;
        }
        return segmentBuffer;
    }
}
