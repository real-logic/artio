package uk.co.real_logic.fix_gateway.commands;

import uk.co.real_logic.fix_gateway.framer.Sender;

final class SenderDisconnect implements SenderCommand
{
    private final long connectionId;

    public SenderDisconnect(final long connectionId)
    {
        this.connectionId = connectionId;
    }

    public void execute(final Sender sender)
    {
        sender.onDisconnect(connectionId);
    }
}
