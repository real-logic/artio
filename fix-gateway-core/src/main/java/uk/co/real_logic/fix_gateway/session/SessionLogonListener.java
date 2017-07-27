package uk.co.real_logic.fix_gateway.session;


import io.aeron.logbuffer.ControlledFragmentHandler.Action;

@FunctionalInterface
public interface SessionLogonListener {
    Action onLogon(Session session);
}
