package uk.co.real_logic.fix_gateway.session;

@FunctionalInterface
public interface SessionLogonListener
{
    void onLogon(Session session);
}
