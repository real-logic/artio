package uk.co.real_logic.artio.engine.framer;

import org.agrona.ErrorHandler;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.util.CharFormatter;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static uk.co.real_logic.artio.CommonConfiguration.DEBUG_BLOCK_SAMPLE_THRESHOLD;
import static uk.co.real_logic.artio.LogTag.BLOCK_SAMPLING;

/**
 * Parent of the different peekers, that lets you control/block a position.
 */
abstract class BlockablePosition
{
    private static final boolean IS_BLOCK_SAMPLING_ENABLED = DebugLogger.isEnabled(BLOCK_SAMPLING);

    static final int DID_NOT_BLOCK = 0;

    private final ErrorHandler errorHandler;
    private final CharFormatter blockSampleFormatter;

    final int maxPayload;

    private long blockPosition;
    private long minPosition;
    private long maxPosition;

    private long lastBlockPosition;
    private long blockSampleCount;

    BlockablePosition(final int maxPayload, final ErrorHandler errorHandler)
    {
        this.maxPayload = maxPayload;
        this.errorHandler = errorHandler;

        blockSampleFormatter = IS_BLOCK_SAMPLING_ENABLED ?
            new CharFormatter("pos=%s, peekId=%s, trace=%s") : null;
    }

    void blockPosition(final long blockPosition, final boolean slow)
    {
        // Pick the lowest (ie first) position to block at
        // We validate the position here, rather than leave it to Aeron as if there's a bug in the position calculation
        // it makes clear what part of the code calculated the incorrect position, incorrect positions are also ignored
        // which keeps Artio going.

        if (validPosition(blockPosition, slow) && this.blockPosition == DID_NOT_BLOCK)
        {
            this.blockPosition = blockPosition;

            if (IS_BLOCK_SAMPLING_ENABLED && blockPosition != DID_NOT_BLOCK)
            {
                if (blockPosition == lastBlockPosition)
                {
                    blockSampleCount++;

                    if (blockSampleCount == DEBUG_BLOCK_SAMPLE_THRESHOLD)
                    {
                        DebugLogger.log(BLOCK_SAMPLING, blockSampleFormatter.clear()
                            .with(blockPosition)
                            .with(peekSessionId())
                            .with(Exceptions.getStackTrace()));
                    }
                }
                else
                {
                    lastBlockPosition = blockPosition;
                    blockSampleCount = 0;
                }
            }
        }
    }

    boolean validPosition(final long position, final boolean slow)
    {
        final long maxPosition = this.maxPosition;
        if (position > maxPosition && !slow)
        {
            // Doesn't block if you aren't slow and over the max position because your slow subscription hasn't hit
            // that point yet, by the time you hit that position on the slow subscription you will end up blocking
            // also not an error, so don't log it.
            return false;
        }

        if (position < minPosition || position > maxPosition)
        {
            errorHandler.onError(new IllegalArgumentException(
                position + " position out of range: " + minPosition + "-" + maxPosition));

            return false;
        }

        if (0 != (position & (FRAME_ALIGNMENT - 1)))
        {
            errorHandler.onError(new IllegalArgumentException(position + " position not aligned to FRAME_ALIGNMENT"));

            return false;
        }

        return true;
    }

    long blockPosition()
    {
        return blockPosition;
    }

    void startPeek(final long minPosition, final long maxPosition)
    {
        this.blockPosition = DID_NOT_BLOCK;
        this.minPosition = minPosition;
        this.maxPosition = maxPosition;
    }

    protected abstract int peekSessionId();

}
