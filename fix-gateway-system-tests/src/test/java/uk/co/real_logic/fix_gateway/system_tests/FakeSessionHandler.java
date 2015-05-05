package uk.co.real_logic.fix_gateway.system_tests;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.session.NewSessionHandler;
import uk.co.real_logic.fix_gateway.session.SessionHandler;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.otf.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.otf.OtfParser;
import uk.co.real_logic.fix_gateway.replication.GatewaySubscription;

public class FakeSessionHandler implements SessionHandler, NewSessionHandler
{

    private final OtfParser parser;

    private Session session;
    private GatewaySubscription subscription;
    private long connectionId = -1;

    public FakeSessionHandler(final OtfMessageAcceptor acceptor)
    {
        parser = new OtfParser(acceptor, new IntDictionary());
    }

    public void onMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long connectionId,
        final long sessionId,
        final int messageType)
    {
        parser.onMessage(buffer, offset, length);
    }

    public void onDisconnect(final long connectionId)
    {
        this.connectionId = connectionId;
    }

    public long connectionId()
    {
        return connectionId;
    }

    public void onConnect(final Session session, final GatewaySubscription subscription)
    {
        this.session = session;
        this.subscription = subscription;
        subscription.sessionHandler(this);
    }

    public GatewaySubscription subscription()
    {
        return subscription;
    }

    public Session session()
    {
        return session;
    }
}
