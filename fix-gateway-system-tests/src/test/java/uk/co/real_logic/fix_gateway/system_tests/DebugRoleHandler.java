package uk.co.real_logic.fix_gateway.system_tests;

import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.replication.RoleHandler;

import static uk.co.real_logic.fix_gateway.LogTag.GATEWAY_CLUSTER_TEST;

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
