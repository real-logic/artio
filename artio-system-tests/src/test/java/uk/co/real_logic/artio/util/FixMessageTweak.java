package uk.co.real_logic.artio.util;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Easily generate valid FIX messages from captured data.
 */
public class FixMessageTweak
{

    /**
     * Replaces sending time with current time and recompute BodyLength / Checksum.
     *
     * @param asciiMsg a FIX message where SOH character may have been replaced with '|'
     * @return a FIX message ready to send on a socket
     */
    public static byte[] recycle(final String asciiMsg)
    {
        String msg = asciiMsg;
        msg = msg.replace('|', '\001');

        // Replace Sending Time (52) with current time
        final String time = LocalDateTime
            .now(ZoneOffset.UTC)
            .format(DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS"));
        msg = msg.replaceAll("52=[^\u0001]*\u0001", "52=" + time + '\001');

        // recompute body length
        final int body = msg.indexOf('\001', 10) + 1;
        final int trailer = msg.indexOf("10=");
        msg = msg.replaceAll("9=\\d+", "9=" + (trailer - body));

        // recompute checksum
        msg = msg.replaceAll("10=[0-9]{3}", "10=" + computeChecksum(msg));

        return msg.getBytes(StandardCharsets.UTF_8);
    }

    private static String computeChecksum(final String fixMessage)
    {
        int checksum = 0;
        for (int i = fixMessage.indexOf("10=") - 1; i >= 0; i--)
        {
            checksum += fixMessage.charAt(i);
        }
        return String.format("%03d", checksum % 256);
    }
}
