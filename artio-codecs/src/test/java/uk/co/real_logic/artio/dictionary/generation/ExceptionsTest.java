package uk.co.real_logic.artio.dictionary.generation;

import org.junit.Test;

import java.io.IOException;
import java.net.SocketException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.dictionary.generation.Exceptions.isJustDisconnect;

public class ExceptionsTest
{
    @Test
    public void testIsJustDisconnect()
    {
        assertTrue(isJustDisconnect(new IOException("Connection reset by peer")));
        assertTrue(isJustDisconnect(new SocketException("Connection reset"))); // Java >= 13
        assertFalse(isJustDisconnect(new IOException()));
        assertFalse(isJustDisconnect(new IOException("Error")));
    }
}
