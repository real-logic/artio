package uk.co.real_logic.fix_gateway.commands;

import uk.co.real_logic.fix_gateway.framer.Receiver;

public class ReceiverDisconnect implements ReceiverCommand
{
    private final long connectionId;

    public ReceiverDisconnect(final long connectionId)
    {
        this.connectionId = connectionId;
    }

    public void execute(final Receiver receiver)
    {
        receiver.onDisconnect(connectionId);
    }
}
