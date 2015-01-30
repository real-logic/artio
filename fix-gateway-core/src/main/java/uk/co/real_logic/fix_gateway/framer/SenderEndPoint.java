package uk.co.real_logic.fix_gateway.framer;

import java.nio.channels.SocketChannel;

/**
 * .
 */
public class SenderEndPoint
{
    private final SocketChannel channel;

    public SenderEndPoint(SocketChannel channel)
    {
        this.channel = channel;
    }
}
