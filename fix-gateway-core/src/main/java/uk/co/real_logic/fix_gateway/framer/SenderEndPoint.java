package uk.co.real_logic.fix_gateway.framer;

import uk.co.real_logic.agrona.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * .
 */
public class SenderEndPoint implements MessageHandler
{
    private final SocketChannel channel;

    public SenderEndPoint(SocketChannel channel)
    {
        this.channel = channel;
    }

    public void onMessage(final DirectBuffer directBuffer, final int offset, final int length)
    {
        final ByteBuffer buffer = directBuffer.byteBuffer();
        buffer.position(offset);
        buffer.limit(offset + length);
        try
        {
            int bytesWritten = 0;
            while (bytesWritten < length)
            {
                bytesWritten += channel.write(buffer);
                // TODO: figure backoff strategy
            }
        }
        catch (IOException e)
        {
            // TODO
            e.printStackTrace();
        }
    }
}
