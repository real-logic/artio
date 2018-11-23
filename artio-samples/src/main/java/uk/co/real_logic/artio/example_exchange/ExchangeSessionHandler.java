package uk.co.real_logic.artio.example_exchange;

import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.OrdType;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.builder.ExecutionReportEncoder;
import uk.co.real_logic.artio.decoder.NewOrderSingleDecoder;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

public class ExchangeSessionHandler implements SessionHandler
{
    private static final DecimalFloat MINIMUM_PRICE = new DecimalFloat().fromLong(100);

    private final NewOrderSingleDecoder newOrderSingle = new NewOrderSingleDecoder();
    private final ExecutionReportEncoder executionReport = new ExecutionReportEncoder();
    private final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer();

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
        asciiBuffer.wrap(buffer, offset, length);

        switch (messageType)
        {
            case NewOrderSingleDecoder.MESSAGE_TYPE:
            {
                newOrderSingle.decode(asciiBuffer, 0, length);

                // Perform business validation on some fields
                final Side side = newOrderSingle.sideAsEnum();
                if (side != Side.BUY)
                {
                    break;
                }

                final String symbol = newOrderSingle.symbolAsString();
                if (!"MSFT".equals(symbol))
                {
                    break;
                }

                final OrdType ordType = newOrderSingle.ordTypeAsEnum();
                if (ordType != OrdType.MARKET)
                {
                    break;
                }

                final DecimalFloat price = newOrderSingle.price();
                if (MINIMUM_PRICE.compareTo(price) < 0)
                {
                    break;
                }

                if (ThreadLocalRandom.current().nextDouble() > 0.25)
                {
                    break;
                }

                // TODO: buy
            }
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
