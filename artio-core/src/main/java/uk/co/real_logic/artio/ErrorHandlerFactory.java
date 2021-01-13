package uk.co.real_logic.artio;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.errors.DistinctErrorLog;

import static io.aeron.driver.Configuration.ERROR_BUFFER_LENGTH_PROP_NAME;

public interface ErrorHandlerFactory
{
    static ErrorHandlerFactory saveDistinctErrors()
    {
        return errorBuffer ->
        {
            final EpochClock clock = new SystemEpochClock();
            final DistinctErrorLog distinctErrorLog = new DistinctErrorLog(errorBuffer, clock);
            return (throwable) ->
            {
                if (!distinctErrorLog.record(throwable))
                {
                    System.err.println("Error Log is full, consider increasing " + ERROR_BUFFER_LENGTH_PROP_NAME);
                    throwable.printStackTrace();
                }
            };
        };
    }

    ErrorHandler make(AtomicBuffer errorBuffer);
}
