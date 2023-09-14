package uk.co.real_logic.artio.engine.framer;

import uk.co.real_logic.artio.Reply;

final class EngineStreamInfoRequestCommand implements AdminCommand, Reply<EngineStreamInfo>
{
    private volatile State state = State.EXECUTING;

    private EngineStreamInfo engineStreamInfo;

    public Throwable error()
    {
        return null;
    }

    public EngineStreamInfo resultIfPresent()
    {
        return engineStreamInfo;
    }

    public State state()
    {
        return state;
    }

    public void execute(final Framer framer)
    {
        framer.onEngineStreamInfoRequest(this);
    }

    public void complete(final EngineStreamInfo engineStreamInfo)
    {
        this.engineStreamInfo = engineStreamInfo;
        state = State.COMPLETED;
    }
}
