package uk.co.real_logic.artio.engine.framer;

import org.agrona.ErrorHandler;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;

/**
 * Parent of the different peekers, that lets you control/block a position.
 */
class BlockablePosition
{
    static final int DID_NOT_BLOCK = 0;

    private final ErrorHandler errorHandler;

    final int maxPayload;

    private long blockPosition;
    private long minPosition;
    private long maxPosition;

    BlockablePosition(final int maxPayload, final ErrorHandler errorHandler)
    {
        this.maxPayload = maxPayload;
        this.errorHandler = errorHandler;
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

}
