package uk.co.real_logic.artio.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A server that accepts TCP connections and is able to reply automatically with canned
 * data. It can be used to simulate a FIX server in order to quickly sent specific messages.
 */
public class DebugServer
{
    private final int port;
    private final Queue<byte[]> connectResponses;
    private final BlockingQueue<HasIOStream> clients;
    private final ServerSocket serverSocket;

    /**
     * If true, wait until some data is received before sending prepared messages.
     */
    private boolean waitForData;

    /**
     * Creates a debug server listening on specified port.
     *
     * @param port TCP listening port
     */
    public DebugServer(final int port) throws IOException
    {
        this.port = port;
        this.connectResponses = new ConcurrentLinkedQueue<>();
        this.clients = new LinkedBlockingQueue<>();
        this.serverSocket = new ServerSocket(port);
    }

    /**
     * Adds a message that must be directly sent to connecting clients. Messages
     * are sent in the same order they were added.
     *
     * @param message binary message to send to new clients
     */
    public void addConnectResponse(final byte[] message)
    {
        connectResponses.add(message);
    }

    /**
     * Warning: causes problems because SendingTime and checksum needs to be regenerated
     * and they are not.
     *
     * @param message FIX message to automatically send to new clients
     */
    public void addFIXResponse(final String... message)
    {
        for (final String msg : message)
        {
            addConnectResponse(FixMessageTweak.recycle(msg));
        }
    }

    /**
     * Starts the debug server, accepting incoming connections and sending
     * prepared data.
     */
    public void start() throws IOException
    {
        new Thread("DebugServer-" + port)
        {
            @Override
            public void run()
            {
                try
                {
                    while (!serverSocket.isClosed())
                    {
                        final Socket socket = serverSocket.accept();
                        System.out.println("Connection accepted from " + socket.getInetAddress());
                        try
                        {
                            final BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
                            final BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());

                            if (!connectResponses.isEmpty() && waitForData)
                            {
                                in.mark(0);
                                in.read();
                                in.reset();
                            }

                            final HasIOStream client = new HasIOStream(socket, in, out);
                            sendResponses(client.out);
                            clients.add(client);
                        }
                        catch (final IOException e)
                        {
                            e.printStackTrace();
                        }
                    }
                }
                catch (final IOException e)
                {
                    if (!serverSocket.isClosed())
                    {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
    }

    public void stop() throws IOException
    {
        serverSocket.close();
    }

    /**
     * Sends prepared data to the client.
     *
     * @param outputStream output stream for client
     */
    private void sendResponses(final OutputStream outputStream) throws IOException
    {
        for (final byte[] response : connectResponses)
        {
            outputStream.write(response);
            outputStream.flush();
        }
    }

    public HasIOStream popClient(final long timeoutMs) throws InterruptedException
    {
        return clients.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public int getPort()
    {
        return port;
    }

    public void setWaitForData(final boolean waitForData)
    {
        this.waitForData = waitForData;
    }

    public static class HasIOStream
    {

        public final Socket socket;
        public final InputStream in;
        public final OutputStream out;

        public HasIOStream(final Socket socket, final InputStream in, final OutputStream out)
        {
            this.socket = socket;
            this.in = in;
            this.out = out;
        }
    }
}
