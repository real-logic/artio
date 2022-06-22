package uk.co.real_logic.artio.system_tests;

import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.session.Session;

import java.util.function.BiConsumer;

public class FakeDisconnectHandler implements BiConsumer<Session, DisconnectReason>
{
    private Session session;
    private DisconnectReason reason;
    private long timeInMs;

    public void accept(final Session session, final DisconnectReason reason)
    {
        this.session = session;
        this.reason = reason;
        this.timeInMs = System.currentTimeMillis();
    }

    public Session session()
    {
        return session;
    }

    public DisconnectReason reason()
    {
        return reason;
    }

    public long timeInMs()
    {
        return timeInMs;
    }
}
