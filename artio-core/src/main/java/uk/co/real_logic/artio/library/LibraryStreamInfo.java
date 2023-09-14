package uk.co.real_logic.artio.library;

public final class LibraryStreamInfo
{
    private final int inboundPublicationSessionId;
    private final long inboundPublicationPosition;
    private final int outboundPublicationSessionId;
    private final long outboundPublicationPosition;

    LibraryStreamInfo(
        final int inboundPublicationSessionId,
        final long inboundPublicationPosition,
        final int outboundPublicationSessionId,
        final long outboundPublicationPosition)
    {
        this.inboundPublicationSessionId = inboundPublicationSessionId;
        this.inboundPublicationPosition = inboundPublicationPosition;
        this.outboundPublicationSessionId = outboundPublicationSessionId;
        this.outboundPublicationPosition = outboundPublicationPosition;
    }

    public int inboundPublicationSessionId()
    {
        return inboundPublicationSessionId;
    }

    public long inboundPublicationPosition()
    {
        return inboundPublicationPosition;
    }

    public int outboundPublicationSessionId()
    {
        return outboundPublicationSessionId;
    }

    public long outboundPublicationPosition()
    {
        return outboundPublicationPosition;
    }

    public String toString()
    {
        return "LibraryStreamInfo{" +
            "inboundPublicationSessionId=" + inboundPublicationSessionId +
            ", inboundPublicationPosition=" + inboundPublicationPosition +
            ", outboundPublicationSessionId=" + outboundPublicationSessionId +
            ", outboundPublicationPosition=" + outboundPublicationPosition +
            '}';
    }
}
