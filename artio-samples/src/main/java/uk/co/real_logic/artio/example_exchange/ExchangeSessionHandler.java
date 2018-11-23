package uk.co.real_logic.artio.example_exchange;

import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

public class ExchangeSessionHandler implements SessionHandler
{
    private static final int MINIMUM_PRICE = 100;

    private final Session session;

    ExchangeSessionHandler(final Session session)
    {
        this.session = session;
    }

    @Override
    public ControlledFragmentHandler.Action onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final int libraryId,
        final Session session,
        final int sequenceIndex,
        final int messageType,
        final long timestampInNs,
        final long position)
    {
        switch (messageType)
        {
        }

        return CONTINUE;
    }

    @Override
    public void onSessionStart(final Session session)
    {

    }

    @Override
    public void onTimeout(final int libraryId, final Session session)
    {

    }

    @Override
    public void onSlowStatus(final int libraryId, final Session session, final boolean hasBecomeSlow)
    {

    }

    @Override
    public ControlledFragmentHandler.Action onDisconnect(
        final int libraryId, final Session session, final DisconnectReason reason)
    {
        return CONTINUE;
    }
}
