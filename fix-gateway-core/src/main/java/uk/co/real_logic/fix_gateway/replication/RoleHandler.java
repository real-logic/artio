/*
 * Copyright 2015-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.fix_gateway.replication;

/**
 * Handler for changes to the state of the node. A callback for each event change.
 *
 * The leadership term is provided so as to give an ordered sequence of events. If you
 * need to message an external system about these events then this ordering can be used
 * to disambiguate messages from different nodes in the cluster.
 */
public interface RoleHandler
{
    /**
     * Invoked when the node that the handler has been registered with transitions to a leader.
     *
     * @param leadershipTerm the current leadership term.
     */
    void onTransitionToLeader(int leadershipTerm);

    /**
     * Invoked when the node that the handler has been registered with transitions to a follower.
     *
     * @param leadershipTerm the current leadership term.
     */
    void onTransitionToFollower(int leadershipTerm);

    /**
     * Invoked when the node that the handler has been registered with transitions to a candidate.
     *
     * @param leadershipTerm the current leadership term.
     */
    void onTransitionToCandidate(int leadershipTerm);
}
