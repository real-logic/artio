package uk.co.real_logic.artio.engine;

import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.framer.EngineStreamInfo;

public final class FixEngineInternals
{
    public static Reply<EngineStreamInfo> engineStreamInfo(final FixEngine fixEngine)
    {
        return fixEngine.engineStreamInfo();
    }

    private FixEngineInternals()
    {
    }
}
