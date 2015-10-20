package uk.co.real_logic.fix_gateway.replication;

// TODO: hold config information for aeron streams
public class TermState
{
    private int previousLeadershipSessionId;
    private int leadershipSessionId;
    private int leadershipTerm;
    private long position;

    public TermState previousLeadershipSessionId(int previousLeadershipSessionId)
    {
        this.previousLeadershipSessionId = previousLeadershipSessionId;
        return this;
    }

    public TermState leadershipSessionId(int leadershipSessionId)
    {
        this.leadershipSessionId = leadershipSessionId;
        return this;
    }

    public TermState leadershipTerm(int leadershipTerm)
    {
        this.leadershipTerm = leadershipTerm;
        return this;
    }

    public TermState position(long position)
    {
        this.position = position;
        return this;
    }

    public int previousLeadershipSessionId()
    {
        return previousLeadershipSessionId;
    }

    public int leadershipSessionId()
    {
        return leadershipSessionId;
    }

    public int leadershipTerm()
    {
        return leadershipTerm;
    }

    public long position()
    {
        return position;
    }
}
