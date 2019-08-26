package uk.co.real_logic.artio.decoder;

import uk.co.real_logic.artio.util.AsciiBuffer;

public interface SessionHeaderDecoder
{
    char[] beginString();

    int beginStringLength();

    int msgSeqNum();

    boolean hasOrigSendingTime();

    byte[] origSendingTime();

    int msgTypeLength();

    char[] msgType();

    boolean hasPossResend();

    boolean possResend();

    boolean possDupFlag();

    boolean hasPossDupFlag();

    byte[] sendingTime();

    char[] targetCompID();

    int targetCompIDLength();

    char[] senderCompID();

    int senderCompIDLength();

    void reset();

    int decode(AsciiBuffer asciiBuffer, int offset, int length);
}
