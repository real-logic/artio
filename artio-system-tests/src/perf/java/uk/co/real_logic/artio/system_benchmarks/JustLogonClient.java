package uk.co.real_logic.artio.system_benchmarks;

import uk.co.real_logic.artio.decoder.LogoutDecoder;
import uk.co.real_logic.artio.system_tests.TestFixConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static uk.co.real_logic.artio.system_benchmarks.BenchmarkConfiguration.BAD_LOGON_NO_LINGER;

public class JustLogonClient
{
    public static void main(final String[] args) throws IOException
    {
        int logonCount = 0;

        while (true)
        {
            System.out.println("Attempting logon:");
            final int port = BenchmarkConfiguration.PORT;
            try (TestFixConnection testFixConnection = new TestFixConnection(SocketChannel.open(
                new InetSocketAddress("localhost", port)),
                BenchmarkConfiguration.INITIATOR_ID,
                BenchmarkConfiguration.ACCEPTOR_ID))
            {
                final boolean badLogon = BenchmarkConfiguration.BAD_LOGON;
                System.out.println("badLogon = " + badLogon);
                testFixConnection.password(badLogon ? "wrong_password" :
                    BenchmarkConfiguration.VALID_PASSWORD);
                testFixConnection.logon(false);
                final LogoutDecoder logout = testFixConnection.readLogout();
                System.out.println(logout);

                // Linger connection for maximum amount of time possible
                if (badLogon && !BAD_LOGON_NO_LINGER)
                {
                    testFixConnection.awaitDisconnect();
                }
            }
            logonCount++;
            System.out.println("Disconnected: " + logonCount);

            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(20));
        }
    }
}
