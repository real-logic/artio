package uk.co.real_logic.artio.system_tests;

import org.junit.Test;
import uk.co.real_logic.artio.session.Session;

import java.io.IOException;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.system_tests.FixConnection.*;
import static uk.co.real_logic.artio.system_tests.FixConnection.PROXY_V2_IPV6_SOURCE_PORT;

public class ProxyProtocolSystemTest extends AbstractMessageBasedAcceptorSystemTest
{
    @Test
    public void shouldSupportProxyV1Protocol() throws IOException
    {
        shouldSupportProxyProtocol(FixConnection::sendProxyV1Line, PROXY_SOURCE_IP, PROXY_SOURCE_PORT);
    }

    @Test
    public void shouldSupportProxyV1ProtocolLargest() throws IOException
    {
        shouldSupportProxyProtocol(
            FixConnection::sendProxyV1LargestLine, LARGEST_PROXY_SOURCE_IP, LARGEST_PROXY_SOURCE_PORT);
    }

    @Test
    public void shouldSupportProxyV2ProtocolTcpV4() throws IOException
    {
        shouldSupportProxyProtocol(FixConnection::sendProxyV2LineTcpV4, PROXY_SOURCE_IP, PROXY_V2_SOURCE_PORT);
    }

    @Test
    public void shouldSupportProxyV2ProtocolTcpV6() throws IOException
    {
        shouldSupportProxyProtocol(
            FixConnection::sendProxyV2LineTcpV6, PROXY_V2_IPV6_SOURCE_IP, PROXY_V2_IPV6_SOURCE_PORT);
    }

    @Test
    public void shouldSupportProxyV2ProtocolTcpV6Localhost() throws IOException
    {
        shouldSupportProxyProtocol(
            FixConnection::sendProxyV2LineTcpV6Localhost, "::1", PROXY_V2_IPV6_SOURCE_PORT);
    }

    private void shouldSupportProxyProtocol(
        final Consumer<FixConnection> sendLine, final String proxySourceIp, final int proxySourcePort)
        throws IOException
    {
        setup(true, true);

        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            sendLine.accept(connection);
            logon(connection);

            final Session session = acquireSession();

            assertEquals(proxySourceIp, session.connectedHost());
            assertEquals(proxySourcePort, session.connectedPort());
        }
    }
}
