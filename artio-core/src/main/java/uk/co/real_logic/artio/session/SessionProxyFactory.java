package uk.co.real_logic.artio.session;

import org.agrona.concurrent.EpochClock;
import uk.co.real_logic.artio.protocol.GatewayPublication;

public interface SessionProxyFactory
{
    SessionProxy make(
        final int sessionBufferSize,
        final GatewayPublication gatewayPublication,
        final SessionIdStrategy sessionIdStrategy,
        final SessionCustomisationStrategy customisationStrategy,
        final EpochClock clock,
        final long connectionId,
        final int libraryId);
}
