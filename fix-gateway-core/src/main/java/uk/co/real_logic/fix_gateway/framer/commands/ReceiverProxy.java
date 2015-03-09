package uk.co.real_logic.fix_gateway.framer.commands;

import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.fix_gateway.framer.ReceiverEndPoint;

public class ReceiverProxy
{
    private final OneToOneConcurrentArrayQueue<ReceiverCommand> commandQueue;

    public ReceiverProxy(final OneToOneConcurrentArrayQueue<ReceiverCommand> commandQueue)
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
