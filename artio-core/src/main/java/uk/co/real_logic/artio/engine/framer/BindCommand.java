package uk.co.real_logic.artio.engine.framer;

import uk.co.real_logic.artio.Reply;

class BindCommand implements AdminCommand, Reply<Void>
{
    private volatile State state = State.EXECUTING;

    // thread-safe publication by writes to state after, and reads of state before its read.
    private Exception error;

    public void execute(final Framer framer)
    {
        framer.onBind(this);
    }

    void success()
    {
        state = State.COMPLETED;
    }

    void onError(final Exception error)
    {
        this.error = error;
        state = State.ERRORED;
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


    public String toString()
    {
        return "BindCommand{" +
            ", state=" + state +
            ", error=" + error +
            '}';
    }
}
