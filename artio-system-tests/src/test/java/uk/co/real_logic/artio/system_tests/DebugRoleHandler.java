package uk.co.real_logic.artio.system_tests;

import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.replication.RoleHandler;

import static uk.co.real_logic.artio.LogTag.GATEWAY_CLUSTER_TEST;

class DebugRoleHandler implements RoleHandler
{
    private final int nodeId;

    DebugRoleHandler(final int nodeId)
    {
        this.nodeId = nodeId;
    }

    public void onTransitionToLeader(final int leadershipTerm)
    {
        DebugLogger.log(GATEWAY_CLUSTER_TEST, "%d has become leader%n", nodeId);
    }

    public void onTransitionToFollower(final int leadershipTerm)
    {

    }

    public void onTransitionToCandidate(final int leadershipTerm)
    {

    }
}
