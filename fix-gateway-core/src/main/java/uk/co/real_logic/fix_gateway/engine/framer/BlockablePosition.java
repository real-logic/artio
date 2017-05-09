package uk.co.real_logic.fix_gateway.engine.framer;

/**
 * Parent of the different peekers, that lets you control/block a position.
 */
class BlockablePosition
{
    static final int DID_NOT_BLOCK = 0;

    long blockPosition;

    void blockPosition(final long blockPosition)
    {
        // Pick the lowest (ie first) position to block at
        if (this.blockPosition == DID_NOT_BLOCK)
        {
            this.blockPosition = blockPosition;
        }
    }

}
