package uk.co.real_logic.artio.decoder;

import uk.co.real_logic.artio.builder.Decoder;

public interface AbstractLogonDecoder extends Decoder
{
    int heartBtInt();

    String usernameAsString();

    String passwordAsString();

    boolean hasResetSeqNumFlag();

    boolean resetSeqNumFlag();
}
