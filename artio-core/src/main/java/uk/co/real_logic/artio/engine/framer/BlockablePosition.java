package uk.co.real_logic.artio.engine.framer;

/**
 * Parent of the different peekers, that lets you control/block a position.
 */
class BlockablePosition
{
    static final int DID_NOT_BLOCK = 0;

    final int maxPayload;

    long blockPosition;

    BlockablePosition(final int maxPayload)
    {
        this.maxPayload = maxPayload;
    }

    void blockPosition(final long blockPosition)
    {
        // Pick the lowest (ie first) position to block at
        if (this.blockPosition == DID_NOT_BLOCK)
        {
            this.blockPosition = blockPosition;
        }
    }

}
