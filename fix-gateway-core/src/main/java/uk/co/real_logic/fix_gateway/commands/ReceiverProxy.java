package uk.co.real_logic.fix_gateway.commands;

import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.fix_gateway.framer.ReceiverEndPoint;

import java.util.Queue;

public class ReceiverProxy
{
    private final Queue<ReceiverCommand> commandQueue;
    private final AtomicCounter fails;

    public ReceiverProxy(final Queue<ReceiverCommand> commandQueue, final AtomicCounter fails)
    {
        this.commandQueue = commandQueue;
        this.fails = fails;
    }

    public void newInitiatedConnection(final ReceiverEndPoint receiverEndPoint)
    {
        offer(new NewInitiatedConnection(receiverEndPoint));
    }

    private void offer(final ReceiverCommand command)
    {
        while (!commandQueue.offer(command))
        {
            fails.increment();
            // TODO: decide on retry/backoff strategy
        }
    }
}
