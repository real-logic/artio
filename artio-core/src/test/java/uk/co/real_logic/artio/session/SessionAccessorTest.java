package uk.co.real_logic.artio.session;

import org.junit.Test;

public class SessionAccessorTest
{
    @Test(expected = IllegalStateException.class)
    public void shouldBanAccessOutsideTheLibrary()
    {
        new SessionAccessor(Session.class);
    }
}
