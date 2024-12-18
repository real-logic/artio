package uk.co.real_logic.artio.engine.logger;

import io.aeron.driver.DutyCycleTracker;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochNanoClock;

public class IndexerDutyCycleTracker implements Agent
{
    final String agentNamePrefix;
    final EpochNanoClock clock;
    final DutyCycleTracker dutyCycleTracker;

    public IndexerDutyCycleTracker(final String agentNamePrefix,
                                   final EpochNanoClock clock,
                                   final DutyCycleTracker dutyCycleTracker)
    {
        this.agentNamePrefix = agentNamePrefix;
        this.clock = clock;
        this.dutyCycleTracker = dutyCycleTracker;
    }

    @Override
    public void onStart()
    {
        dutyCycleTracker.update(clock.nanoTime());
    }

    @Override
    public int doWork() throws Exception
    {
        dutyCycleTracker.measureAndUpdate(clock.nanoTime());
        return 0;
    }

    @Override
    public String roleName()
    {
        return agentNamePrefix + "IndexerDutyCycleTracker";
    }
}
