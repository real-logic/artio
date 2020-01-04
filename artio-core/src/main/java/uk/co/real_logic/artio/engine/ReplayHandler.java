package uk.co.real_logic.artio.engine;

import org.agrona.DirectBuffer;

/**
 * A callback that can be implemented to inspect the messages that get replayed.
 *
 * This callback is called for every message that needs to be replayed, even those that are replaced with
 * a gap fill message. The handler is invoked on the Replay Agent.
 */
@FunctionalInterface
public interface ReplayHandler
{
    /**
     * Event to indicate that a fix message has arrived to process.
     *  @param buffer the buffer containing the fix message.
     * @param offset the offset in the buffer where the message starts.
     * @param length the length of the message within the buffer.
     * @param libraryId the id of the library which has received this message.
     * @param sessionId the id of the session which has received this message.
     * @param sequenceIndex the sequence index of the message being replayed.
     * @param messageType the FIX msgType field, encoded as an int.
     */
    void onReplayedMessage(
        DirectBuffer buffer,
        int offset,
        int length,
        int libraryId,
        long sessionId,
        int sequenceIndex,
        long messageType);
}
