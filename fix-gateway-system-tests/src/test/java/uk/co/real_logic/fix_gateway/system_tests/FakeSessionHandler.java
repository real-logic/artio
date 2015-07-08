package uk.co.real_logic.fix_gateway.system_tests;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.library.session.NewSessionHandler;
import uk.co.real_logic.fix_gateway.library.session.Session;
import uk.co.real_logic.fix_gateway.library.session.SessionHandler;
import uk.co.real_logic.fix_gateway.otf.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.otf.OtfParser;
import uk.co.real_logic.fix_gateway.replication.DataSubscriber;

public class FakeSessionHandler implements SessionHandler, NewSessionHandler
{

    private final OtfParser parser;
    private final DataSubscriber subscriber = new DataSubscriber(this);

    private Session session;
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

    public SessionHandler onConnect(final Session session)
    {
        this.session = session;
        return this;
    }

    public Session session()
    {
        return session;
    }

    public void resetSession()
    {
        session = null;
    }

}
