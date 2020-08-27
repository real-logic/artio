package uk.co.real_logic.artio.engine.framer;

import uk.co.real_logic.artio.Reply;

class UnbindCommand implements AdminCommand, Reply<Void>
{
    private volatile State state = State.EXECUTING;

    private UnbindCommand concurrentUnbind;

    // thread-safe publication by writes to state after, and reads of state before its read.
    private Exception error;
    private final boolean disconnect;

    UnbindCommand(final boolean disconnect)
    {
        this.disconnect = disconnect;
    }

    public void execute(final Framer framer)
    {
        framer.onUnbind(this);
    }

    void success()
    {
        state = State.COMPLETED;
        if (concurrentUnbind != null)
        {
            concurrentUnbind.success();
        }
    }

    void onError(final Exception error)
    {
        this.error = error;
        state = State.ERRORED;

        if (concurrentUnbind != null)
        {
            concurrentUnbind.onError(error);
        }
    }

    public Exception error()
    {
        return error;
    }

    public Void resultIfPresent()
    {
        return null;
    }

    public State state()
    {
        return state;
    }

    boolean disconnect()
    {
        return disconnect;
    }

    public String toString()
    {
        return "UnbindCommand{" +
            "disconnect=" + disconnect +
            ", state=" + state +
            ", error=" + error +
            '}';
    }

    public void addConcurrentUnbind(final UnbindCommand unbindCommand)
    {
        if (concurrentUnbind == null)
        {
            concurrentUnbind = unbindCommand;
        }
        else
        {
            concurrentUnbind.addConcurrentUnbind(unbindCommand);
        }
    }
}
