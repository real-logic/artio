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

    void blockPosition(final long blockPosition)
    {
        // Pick the lowest (ie first) position to block at
        if (validPosition(blockPosition) && this.blockPosition == DID_NOT_BLOCK)
        {
            this.blockPosition = blockPosition;
        }
    }

    boolean validPosition(final long position)
    {
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
