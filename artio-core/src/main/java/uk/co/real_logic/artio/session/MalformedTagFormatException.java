package uk.co.real_logic.artio.session;

public class MalformedTagFormatException extends RuntimeException
{
    private final int refTagId;

    public MalformedTagFormatException(final int refTagId, final Throwable cause)
    {
        super(cause);
        this.refTagId = refTagId;
    }

    public int refTagId()
    {
        return refTagId;
    }
}
