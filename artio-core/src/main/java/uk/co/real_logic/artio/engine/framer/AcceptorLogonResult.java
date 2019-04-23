package uk.co.real_logic.artio.engine.framer;

import uk.co.real_logic.artio.messages.DisconnectReason;

public interface AcceptorLogonResult
{
    boolean poll();

    boolean isAccepted();

    DisconnectReason reason();
}
