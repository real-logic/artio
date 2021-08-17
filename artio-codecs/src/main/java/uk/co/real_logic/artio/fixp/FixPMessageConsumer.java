package uk.co.real_logic.artio.fixp;

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.ArtioLogHeader;
import uk.co.real_logic.artio.messages.FixPMessageDecoder;

/**
 * Consumer to read FIXP messages from the archive.
 */
@FunctionalInterface
public interface FixPMessageConsumer
{
    /**
     * Callback for receiving FIXP messages.
     *
     * @param fixPMessage the header of the FIXP message containing a local timestamp.
     * @param buffer      the buffer containing the message.
     * @param offset      the offset within the buffer at which your message starts.
     * @param header      a header object that can reflects properties of the original message before it was archived.
     */
    void onMessage(FixPMessageDecoder fixPMessage, DirectBuffer buffer, int offset, ArtioLogHeader header);
}
