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

    char[] targetSubID();

    int targetSubIDLength();

    char[] targetLocationID();

    int targetLocationIDLength();

    char[] senderCompID();

    int senderCompIDLength();

    char[] senderSubID();

    int senderSubIDLength();

    char[] senderLocationID();

    int senderLocationIDLength();

    void reset();

    int decode(AsciiBuffer asciiBuffer, int offset, int length);

    boolean hasSenderLocationID();

    boolean hasSenderSubID();

    boolean hasTargetLocationID();

    boolean hasTargetSubID();
}
