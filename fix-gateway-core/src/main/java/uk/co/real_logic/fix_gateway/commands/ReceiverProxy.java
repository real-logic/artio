package uk.co.real_logic.fix_gateway.commands;

import uk.co.real_logic.fix_gateway.framer.ReceiverEndPoint;

import java.util.Queue;

public class ReceiverProxy
{
    private final Queue<ReceiverCommand> commandQueue;

    public ReceiverProxy(final Queue<ReceiverCommand> commandQueue)
    {
        this.commandQueue = commandQueue;
    }

    public void newInitiatedConnection(final ReceiverEndPoint receiverEndPoint)
    {
        offer(new NewInitiatedConnection(receiverEndPoint));
    }

    private void offer(final ReceiverCommand command)
    {
        // TODO: decide on retry/backoff strategy
        commandQueue.offer(command);
    }
}
