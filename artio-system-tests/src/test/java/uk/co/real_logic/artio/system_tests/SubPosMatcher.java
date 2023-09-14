package uk.co.real_logic.artio.system_tests;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.AeronCounters.DRIVER_SUBSCRIBER_POSITION_TYPE_ID;
import static io.aeron.driver.status.StreamCounter.SESSION_ID_OFFSET;

final class SubPosMatcher
{
    private final CountersReader countersReader;
    private final long registrationId;
    private final int sessionId;
    private final long expectedPosition;
    private int counterId = -1;

    SubPosMatcher(
        final CountersReader countersReader,
        final long registrationId,
        final int sessionId,
        final long expectedPosition)
    {
        this.countersReader = countersReader;
        this.registrationId = registrationId;
        this.sessionId = sessionId;
        this.expectedPosition = expectedPosition;
    }

    public void tryMatch(final int counterId, final int typeId, final DirectBuffer keyBuffer)
    {
        if (typeId == DRIVER_SUBSCRIBER_POSITION_TYPE_ID &&
            countersReader.getCounterRegistrationId(counterId) == registrationId &&
            keyBuffer.getInt(SESSION_ID_OFFSET) == sessionId)
        {
            if (hasCounterId())
            {
                throw new IllegalStateException();
            }
            this.counterId = counterId;
        }
    }

    public boolean hasCounterId()
    {
        return counterId != -1;
    }

    public boolean isCaughtUp()
    {
        final long counterValue = countersReader.getCounterValue(counterId);
        return counterValue >= expectedPosition;
    }

    public String toString()
    {
        return "SubPosMatcher{" +
            "registrationId=" + registrationId +
            ", sessionId=" + sessionId +
            ", expectedPosition=" + expectedPosition +
            ", counterId=" + counterId +
            '}';
    }
}
