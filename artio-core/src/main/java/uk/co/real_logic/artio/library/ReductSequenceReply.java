package uk.co.real_logic.artio.library;

public class ReductSequenceReply extends LibraryReply<Boolean>
{
    private final long sessionId;

    ReductSequenceReply(final LibraryPoller libraryPoller, final long latestReplyArrivalTime, final long sessionId)
    {
        super(libraryPoller, latestReplyArrivalTime);
        this.sessionId = sessionId;

        if (libraryPoller.isConnected())
        {
            sendMessage();
        }

    }

    @Override
    protected void sendMessage()
    {
        final long position = libraryPoller.saveReductSequenceUpdate(sessionId);
        requiresResend = position < 0;

        if (!requiresResend)
        {
            onComplete(true);
        }
    }
}
