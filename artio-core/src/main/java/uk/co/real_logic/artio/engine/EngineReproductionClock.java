package uk.co.real_logic.artio.engine;

import org.agrona.concurrent.EpochNanoClock;

public class EngineReproductionClock implements EpochNanoClock
{
    private long timeInNs;

    public EngineReproductionClock(final long timeInNs)
    {
        this.timeInNs = timeInNs;
    }

    public long nanoTime()
    {
        return timeInNs;
    }

    public void advanceTimeTo(final long timeInNs)
    {
        this.timeInNs = timeInNs;
    }
}
