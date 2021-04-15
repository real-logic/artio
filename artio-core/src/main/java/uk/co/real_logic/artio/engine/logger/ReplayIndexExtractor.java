package uk.co.real_logic.artio.engine.logger;

import org.agrona.IoUtil;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.storage.messages.ReplayIndexRecordDecoder;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static uk.co.real_logic.artio.builder.Encoder.BITS_IN_INT;
import static uk.co.real_logic.artio.engine.logger.ReplayIndexDescriptor.*;
import static uk.co.real_logic.artio.engine.logger.ReplayIndexDescriptor.RECORD_LENGTH;

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
        final long sessionId,
        final boolean inbound,
        final ReplayIndexHandler handler)
    {
        final int streamId = inbound ? configuration.inboundLibraryStream() : configuration.outboundLibraryStream();
        final File file = replayIndexFile(configuration.logFileDir(), sessionId, streamId);
        extract(file, handler);
    }

    public static void extract(final File file, final ReplayIndexHandler handler)
    {
        final MappedByteBuffer mappedByteBuffer = LoggerUtil.mapExistingFile(file);
        try
        {
            final UnsafeBuffer buffer = new UnsafeBuffer(mappedByteBuffer);

            final MessageHeaderDecoder messageFrameHeader = new MessageHeaderDecoder();
            final ReplayIndexRecordDecoder indexRecord = new ReplayIndexRecordDecoder();

            messageFrameHeader.wrap(buffer, 0);
            final int actingBlockLength = messageFrameHeader.blockLength();
            final int actingVersion = messageFrameHeader.version();

            final int capacity = recordCapacity(buffer.capacity());

            long iteratorPosition = beginChangeVolatile(buffer);
            long stopIteratingPosition = iteratorPosition + capacity;

            while (iteratorPosition < stopIteratingPosition)
            {
                final long changePosition = endChangeVolatile(buffer);

                if (changePosition > iteratorPosition && (iteratorPosition + capacity) <= beginChangeVolatile(buffer))
                {
                    handler.onLapped();
                    iteratorPosition = changePosition;
                    stopIteratingPosition = iteratorPosition + capacity;
                }

                final int offset = offset(iteratorPosition, capacity);
                indexRecord.wrap(buffer, offset, actingBlockLength, actingVersion);
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
            IoUtil.unmap(mappedByteBuffer);
        }
    }
}
