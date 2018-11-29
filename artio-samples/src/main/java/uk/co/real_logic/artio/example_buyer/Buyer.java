package uk.co.real_logic.artio.example_buyer;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.OrdType;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.builder.NewOrderSingleEncoder;
import uk.co.real_logic.artio.decoder.ExecutionReportDecoder;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConnectHandler;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.library.SessionHandler;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.server.SampleServer.ACCEPTOR_COMP_ID;
import static uk.co.real_logic.artio.server.SampleServer.INITIATOR_COMP_ID;

public class Buyer implements LibraryConnectHandler, SessionHandler
{
    private enum State
    {
        LIBRARY_DISCONNECTED,
        LIBRARY_CONNECTED,
        SESSION_CONNECTING,
        SESSION_CONNECTED,
        AWAITING_FILL
    }

    private final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final NewOrderSingleEncoder newOrderSingle = new NewOrderSingleEncoder();
    private final ExecutionReportDecoder executionReport = new ExecutionReportDecoder();
    private final DecimalFloat price = new DecimalFloat(100);
    private final DecimalFloat orderQty = new DecimalFloat(2);

    private State state = State.LIBRARY_DISCONNECTED;

    private FixLibrary library;
    private Reply<Session> initiateReply;
    private Session session;

    @Override
    public void onConnect(final FixLibrary library)
    {
        state = State.LIBRARY_CONNECTED;
        this.library = library;
    }

    @Override
    public void onDisconnect(final FixLibrary library)
    {
        state = State.LIBRARY_DISCONNECTED;
        this.library = null;
    }

    public int poll()
    {
        checkReplies();

        switch (state)
        {
            case LIBRARY_CONNECTED:
                connectSession();
                return 1;

            case SESSION_CONNECTED:
                sendOrder();
                return 1;
        }

        return 0;
    }

    private void checkReplies()
    {
        if (initiateReply != null && !initiateReply.isExecuting())
        {
            if (initiateReply.hasCompleted())
            {
                session = initiateReply.resultIfPresent();
                state = State.SESSION_CONNECTED;
            }

            // TODO: error cases

            initiateReply = null;
        }
    }

    private void connectSession()
    {
        // Each outbound session with an Exchange or broker is represented by
        // a Session object. Each session object can be configured with connection
        // details and credentials.
        final SessionConfiguration sessionConfig = SessionConfiguration.builder()
            .address("localhost", 9999)
            .targetCompId(ACCEPTOR_COMP_ID)
            .senderCompId(INITIATOR_COMP_ID)
            .build();

        initiateReply = library.initiate(sessionConfig);

        state = State.SESSION_CONNECTING;
    }

    private void sendOrder()
    {
        newOrderSingle
            .clOrdID("A")
            .side(Side.BUY)
            .transactTime(null)
            .ordType(OrdType.MARKET)
            .price(price);

        newOrderSingle.instrument().symbol("MSFT");
        newOrderSingle.orderQtyData().orderQty(orderQty);

        final long position = session.send(newOrderSingle);
        if (!Pressure.isBackPressured(position))
        {
            state = State.AWAITING_FILL;
        }
    }

    @Override
    public Action onMessage(
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
        if (messageType == ExecutionReportDecoder.MESSAGE_TYPE)
        {
            asciiBuffer.wrap(buffer, offset, length);
            executionReport.decode(asciiBuffer, 0, length);

            System.out.println("Received report: " + executionReport);

            state = State.SESSION_CONNECTED;
        }

        return CONTINUE;
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
    public Action onDisconnect(final int libraryId, final Session session, final DisconnectReason reason)
    {
        state = State.LIBRARY_CONNECTED;

        return CONTINUE;
    }

    @Override
    public void onSessionStart(final Session session)
    {
    }

}
