package uk.co.real_logic.artio.builder;

import uk.co.real_logic.artio.util.AsciiBuffer;

/**
 * Interface for visiting unknown tags.
 *
 * @see CommonDecoderImpl#setUnknownTagVisitor(UnknownTagVisitor)
 */
public interface UnknownTagVisitor
{
    /**
     * Called when an unknown tag is encountered while decoding a message.
     *
     * @param tag    The tag number of the unknown tag
     * @param buffer The buffer containing the unknown tag
     * @param offset The offset at which the unknown tag starts
     * @param length The length of the unknown tag
     */
    void onUnknownTag(
        int tag,
        AsciiBuffer buffer,
        int offset,
        int length
    );
}
