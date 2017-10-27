package uk.co.real_logic.artio.session;

@FunctionalInterface
public interface SessionLogonListener
{
    void onLogon(Session session);
}
