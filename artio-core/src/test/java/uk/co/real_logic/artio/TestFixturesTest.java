package uk.co.real_logic.artio;

import org.junit.jupiter.api.Test;
import uk.co.real_logic.artio.decoder.HeartbeatDecoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestFixturesTest
{
    @Test
    void shouldGenerateMessageOfGivenLength()
    {
        final int messageLength = 256;
        final byte[] bytes = TestFixtures.largeMessage(messageLength);
        assertEquals(messageLength, bytes.length);

        final HeartbeatDecoder decoder = new HeartbeatDecoder();
        final MutableAsciiBuffer buffer = new MutableAsciiBuffer(bytes);
        decoder.decode(buffer, 0, bytes.length);
        assertTrue(decoder.validate());
        assertEquals(buffer.computeChecksum(0, messageLength - 7),
            Integer.parseInt(decoder.trailer().checkSumAsString(), 10));
    }
}
