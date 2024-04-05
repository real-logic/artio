package uk.co.real_logic.artio.util;

import org.junit.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Helper to pop FIX messages received on a socket.
 *
 * @see DebugServer
 */
public class DebugFIXClient
{
    private final DebugServer.HasIOStream io;
    private Thread thread;

    private final BlockingQueue<Map<String, String>> messages = new LinkedBlockingQueue<>();
    private volatile boolean disposed;
    private String prefix = " <<< ";

    public DebugFIXClient(final DebugServer.HasIOStream io)
    {
        this.io = Objects.requireNonNull(io);
    }

    public void start()
    {
        assert thread == null;
        thread = new Thread(this::run, "DebugFIXClient");
        thread.start();
    }

    public void close() throws Exception
    {
        disposed = true;
        io.in.close();
        io.in.close();
        io.socket.close();
        thread.interrupt();
        thread.join();
    }

    private void run()
    {
        final StringBuilder s = new StringBuilder(128);
        while (!disposed)
        {
            final Scanner scanner = new Scanner(io.in).useDelimiter("\u0001");
            Map<String, String> msg = new HashMap<>();
            while (scanner.hasNext())
            {
                final String fld = scanner.next();
                s.append(fld).append('|');
                final int eq = fld.indexOf('=');
                final String tag = fld.substring(0, eq);
                msg.put(tag, fld.substring(eq + 1));
                if (tag.equals("10"))
                {
                    messages.add(msg);
                    msg = new HashMap<>();
                    System.err.println(prefix + s);
                    s.setLength(0);
                }
            }
        }
    }

    public Map<String, String> popMessage() throws InterruptedException
    {
        return messages.poll(5, TimeUnit.SECONDS);
    }

    /**
     * Pop a message and check that it contains some field/value pairs.
     *
     * @param tagValues a string of the form "35=5 58=Bye"
     */
    public void popAndAssert(final String tagValues) throws InterruptedException
    {
        final Map<String, String> map = popMessage();
        System.err.println(map);
        if (map == null)
        {
            throw new AssertionError("No message received");
        }

        for (final String rule : tagValues.split(" "))
        {
            final String tag = rule.substring(0, rule.indexOf('='));
            final String value = map.get(tag);
            try
            {
                Assert.assertEquals(rule, tag + "=" + value);
            }
            catch (final Throwable e)
            {
                e.printStackTrace();
                throw e;
            }
        }
    }

    public void setPrefix(final String prefix)
    {
        this.prefix = prefix;
    }
}
