/*
 * Copyright 2015-2016 Real Logic Ltd.
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

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;
import static uk.co.real_logic.fix_gateway.replication.Follower.NO_ONE;
import static uk.co.real_logic.fix_gateway.replication.ReplicationAsserts.*;

@RunWith(Parameterized.class)
public class ScenariosTest
{
    private static final long TIME = 10L;
    private static final long POSITION = 40L;
    private static final long TIMEOUT_IN_MS = 100;
    private static final long BELOW_TIMEOUT = TIMEOUT_IN_MS - 1;
    private static final int OLD_LEADERSHIP_TERM = 0;
    private static final int LEADERSHIP_TERM = OLD_LEADERSHIP_TERM + 1;
    private static final int NEW_TERM = LEADERSHIP_TERM + 1;
    private static final int SESSION_ID = 42;
    private static final int HEARTBEAT_INTERVAL_IN_MS = 10;
    private static final int NEW_LEADER_SESSION_ID = 43;
    private static final int CLUSTER_SIZE = 5;
    private static final short ID = 2;
    private static final short NEW_LEADER_ID = 3;
    private static final DirectBuffer NODE_STATE_BUFFER = new UnsafeBuffer(new byte[1]);
    private static final int NODE_STATE_LENGTH = 1;

    private static final short CANDIDATE_ID = 5;
    private static final short FOLLOWER_1_ID = 4;
    private static final short FOLLOWER_2_ID = 5;

    private final ClusterAgent clusterNode = mock(ClusterAgent.class);
    private final RaftPublication controlPublication = mock(RaftPublication.class);
    private final RaftPublication acknowledgementPublication = mock(RaftPublication.class);
    private final Subscription controlSubscription = mock(Subscription.class);
    private final Subscription acknowledgementSubscription = mock(Subscription.class);
    private final Subscription dataSubscription = mock(Subscription.class);
    private final Image leaderDataImage = mock(Image.class);
    private final ArchiveReader archiveReader = mock(ArchiveReader.class);
    private final ArchiveReader.SessionReader sessionReader = mock(ArchiveReader.SessionReader.class);
    private final TermState termState = new TermState();
    private final ControlledFragmentHandler fragmentHandler = mock(ControlledFragmentHandler.class);
    private final Archiver.SessionArchiver leaderArchiver = mock(Archiver.SessionArchiver.class);
    private final Archiver archiver = mock(Archiver.class);
    private final NodeStateHandler nodeStateHandler = mock(NodeStateHandler.class);

    private final RoleFixture roleFixture;
    private final Stimulus stimulus;
    private final Effect requiredEffect;
    private final State requiredState;

    private Role role;
    private RaftHandler raftHandler;

    @Parameterized.Parameters(name = "'{'{0}'}' {1} '{'{2}, {3}'}'")
    public static Iterable<Object[]> parameters()
    {
        return Arrays.asList(
            scenario(leader, newLeaderHeartbeat, transitionsToFollower, hasNewLeader(NEW_LEADER_SESSION_ID)),

            scenario(leader, oldTermLeaderHeartbeat, neverTransitions, ignored),

            scenario(leader, selfHeartbeat, neverTransitions, ignored),

            scenario(leader, newLeaderRequestVote, votesAndFollows(CANDIDATE_ID), hasNoLeader(NEW_TERM)),

            scenario(leader, lowerTermRequestVote, neverTransitions, ignored),

            scenario(leader, lowerPositionRequestVote, neverTransitions, ignored),

            scenario(follower, timesOut, transitionsToCandidate, hasNoLeader(LEADERSHIP_TERM)),

            scenario(follower, heartbeatBeforeTimeout, neverTransitions, ignored),

            scenario(follower, newLeaderRequestVote, voteForCandidate, ignored),

            scenario(follower, lowerTermRequestVote, neverTransitions, ignored),

            scenario(follower, lowerPositionRequestVote, neverTransitions, ignored),

            scenario(follower, newLeaderHeartbeat, neverTransitions, hasNewLeader(NEW_LEADER_SESSION_ID)),

            scenario(follower, oldTermLeaderHeartbeat, neverTransitions, ignored),

            scenario(candidate, startElection, requestsVote, ignored),

            scenario(candidate, onMajority, transitionsToLeader, isLeader(SESSION_ID)),

            scenario(candidate, newLeaderRequestVote, votesAndFollows(CANDIDATE_ID), hasNewLeader(SESSION_ID)),

            scenario(candidate, lowerTermRequestVote, neverTransitions, ignored),

            scenario(candidate, lowerPositionRequestVote, neverTransitions, ignored),

            scenario(candidate, newLeaderHeartbeat, transitionsToFollower, hasNewLeader(NEW_LEADER_SESSION_ID)),

            scenario(candidate, oldTermLeaderHeartbeat, neverTransitions, ignored),

            scenario(candidate, selfHeartbeat, neverTransitions, ignored)
        );
    }

    public ScenariosTest(
        final RoleFixture roleFixture,
        final Stimulus stimulus,
        final Effect requiredEffect,
        final State requiredState)
    {
        this.roleFixture = roleFixture;
        this.stimulus = stimulus;
        this.requiredEffect = requiredEffect;
        this.requiredState = requiredState;
    }

    @Test
    public void evaluateState()
    {
        given:
        setup();

        role = roleFixture.apply(this);
        raftHandler = (RaftHandler)role;

        when:
        stimulus.accept(this);

        then:
        requiredEffect.check(this);
        requiredState.accept(termState);
    }

    @FunctionalInterface
    interface RoleFixture extends Function<ScenariosTest, Role>
    {
    }

    @FunctionalInterface
    interface Stimulus extends Consumer<ScenariosTest>
    {
    }

    @FunctionalInterface
    interface Effect
    {
        void check(ScenariosTest st);

        default Effect and(Effect right)
        {
            final Effect left = this;
            return new Effect()
            {
                public void check(final ScenariosTest st)
                {
                    left.check(st);
                    right.check(st);
                }

                public String toString()
                {
                    return left + " and " + right;
                }
            };
        }
    }

    @FunctionalInterface
    interface State extends Consumer<TermState>
    {
    }

    private static State hasNewLeader(final int sessionId)
    {
        return namedState(
            (termState) ->
            {
                assertThat(termState, hasLeaderSessionId(sessionId));
                assertThat(termState, hasLeadershipTerm(NEW_TERM));
                assertThat(termState, hasConsensusPosition(POSITION));
            }, "hasNewLeader");
    }

    private static State isLeader(final int sessionId)
    {
        return namedState(
            (termState) ->
            {
                assertThat(termState, hasLeaderSessionId(sessionId));
                assertThat(termState, hasLeadershipTerm(LEADERSHIP_TERM));
            }, "hasNewLeader");
    }

    private static State hasNoLeader(final int leadershipTerm)
    {
        return namedState(
            (termState) ->
            {
                assertThat(termState, noLeaderMatcher());
                assertThat(termState, hasLeadershipTerm(leadershipTerm));
                assertThat(termState, hasConsensusPosition(POSITION));
            }, "hasNoLeader");
    }

    private static RoleFixture leader = named(ScenariosTest::leader, "leader");

    private static RoleFixture follower = named(ScenariosTest::follower, "follower");

    private static RoleFixture candidate = named(ScenariosTest::candidate, "candidate");

    private Role leader()
    {
        termState
            .leadershipTerm(LEADERSHIP_TERM)
            .consensusPosition(POSITION);

        final Leader leader = new Leader(
            ID,
            new EntireClusterAcknowledgementStrategy(),
            new IntHashSet(40, -1),
            clusterNode,
            0,
            HEARTBEAT_INTERVAL_IN_MS,
            termState,
            SESSION_ID,
            archiveReader,
            new RaftArchiver(termState.leaderSessionId(), archiver),
            NODE_STATE_BUFFER,
            nodeStateHandler);

        leader
            .controlPublication(controlPublication)
            .acknowledgementSubscription(acknowledgementSubscription)
            .dataSubscription(dataSubscription)
            .getsElected(TIME);

        return leader;
    }

    private Follower follower()
    {
        termState
            .allPositions(POSITION)
            .leadershipTerm(LEADERSHIP_TERM)
            .leaderSessionId(SESSION_ID);

        final Follower follower = new Follower(
            ID,
            clusterNode,
            TIME,
            TIMEOUT_IN_MS,
            termState,
            new RaftArchiver(termState.leaderSessionId(), archiver),
            NODE_STATE_BUFFER,
            nodeStateHandler);

        follower
            .controlPublication(controlPublication)
            .acknowledgementPublication(acknowledgementPublication)
            .controlSubscription(controlSubscription)
            .follow(TIME);

        return follower;
    }

    private Candidate candidate()
    {
        termState
            .noLeader()
            .leadershipTerm(OLD_LEADERSHIP_TERM)
            .consensusPosition(POSITION);

        final Candidate candidate = new Candidate(
            ID,
            SESSION_ID,
            clusterNode,
            CLUSTER_SIZE,
            TIMEOUT_IN_MS,
            termState,
            new QuorumAcknowledgementStrategy(), NODE_STATE_BUFFER, nodeStateHandler);

        candidate
            .controlPublication(controlPublication)
            .controlSubscription(controlSubscription)
            .startNewElection(TIME);

        return candidate;
    }

    private static Effect voteForCandidate = namedEffect(st ->
        verify(st.controlPublication).saveReplyVote(ID, CANDIDATE_ID, NEW_TERM, FOR, NODE_STATE_BUFFER),
        "voteForCandidate");

    private static Effect transitionsToFollower =
        transitionsToFollower(NO_ONE, "transitionsToFollower");

    private static Effect votesAndFollows(final int votedFor)
    {
        return voteForCandidate.and(transitionsToFollower(votedFor, "transitionsToFollowerOf" + votedFor));
    }

    private static Effect transitionsToFollower(final int votedFor, final String name)
    {
        return namedEffect(
            (st) ->
            {
                final ClusterAgent node = verify(st.clusterNode, atLeastOnce());
                if (st.role instanceof Leader)
                {
                    final Leader leader = (Leader)st.role;
                    node.transitionToFollower(eq(leader), eq((short)votedFor), anyLong());
                }
                else
                {
                    final Candidate candidate = (Candidate)st.role;
                    node.transitionToFollower(eq(candidate), eq((short)votedFor), anyLong());
                }

                ReplicationAsserts.neverTransitionsToCandidate(st.clusterNode);
                ReplicationAsserts.neverTransitionsToLeader(st.clusterNode);
            }, name);
    }

    private static Effect transitionsToCandidate =
        namedEffect(
            (st) ->
            {
                ReplicationAsserts.transitionsToCandidate(st.clusterNode);

                ReplicationAsserts.neverTransitionsToFollower(st.clusterNode);
                ReplicationAsserts.neverTransitionsToLeader(st.clusterNode);
            },
            "transitionsToCandidate");

    private static Effect transitionsToLeader =
        namedEffect(
            (st) ->
            {
                ReplicationAsserts.transitionsToLeader(st.clusterNode);

                ReplicationAsserts.neverTransitionsToFollower(st.clusterNode);
                ReplicationAsserts.neverTransitionsToCandidate(st.clusterNode);
            },
            "transitionsToLeader");

    private static Effect neverTransitions =
        namedEffect(
            (st) ->
            {
                ReplicationAsserts.neverTransitionsToFollower(st.clusterNode);
                ReplicationAsserts.neverTransitionsToLeader(st.clusterNode);
                ReplicationAsserts.neverTransitionsToCandidate(st.clusterNode);
            }, "neverTransitions");

    public static Effect requestsVote =
        namedEffect(
            (st) ->
            {
                st.requestsVote(LEADERSHIP_TERM);
                ReplicationAsserts.neverTransitionsToFollower(st.clusterNode);
                ReplicationAsserts.neverTransitionsToCandidate(st.clusterNode);
                ReplicationAsserts.neverTransitionsToLeader(st.clusterNode);
            }, "requestsVote");

    public static Stimulus oldTermLeaderHeartbeat =
        receivesHeartbeat(NEW_LEADER_ID, OLD_LEADERSHIP_TERM, NEW_LEADER_SESSION_ID, "oldTermLeaderHeartbeat");

    public static Stimulus newLeaderHeartbeat =
        receivesHeartbeat(NEW_LEADER_ID, NEW_TERM, NEW_LEADER_SESSION_ID, "newLeaderHeartbeat");

    public static Stimulus selfHeartbeat =
        receivesHeartbeat(ID, NEW_TERM, SESSION_ID, "selfHeartbeat");

    private static Stimulus receivesHeartbeat(
        final short leaderId,
        final int leaderShipTerm,
        final int dataSessionId,
        final String name)
    {
        return namedStimulus(
            (st) -> st.raftHandler.onConsensusHeartbeat(leaderId, leaderShipTerm, POSITION, dataSessionId), name);
    }

    private static Stimulus timesOut =
        namedStimulus((st) -> st.role.poll(1, TIME + TIMEOUT_IN_MS * 2 + 1), "timesOut");

    private static Stimulus heartbeatBeforeTimeout =
        namedStimulus(
            (st) ->
            {
                when(st.controlSubscription.controlledPoll(any(), anyInt())).thenAnswer(
                    (inv) ->
                    {
                        st.raftHandler.onConsensusHeartbeat(NEW_LEADER_ID, LEADERSHIP_TERM, POSITION, SESSION_ID);

                        return 1;
                    });

                long time = TIME + BELOW_TIMEOUT;

                st.role.poll(1, time);

                time += BELOW_TIMEOUT;

                st.role.poll(1, time);

                time += BELOW_TIMEOUT;

                st.role.poll(1, time);
            },
            "heartbeatBeforeTimeout");

    private static void heartbeat(final ScenariosTest st)
    {
        st.raftHandler.onConsensusHeartbeat(NEW_LEADER_ID, LEADERSHIP_TERM, POSITION, SESSION_ID);
    }

    private static Stimulus startElection = namedStimulus(
        (st) ->
        {
        }, "startElection");

    private static Stimulus onMajority =
        namedStimulus(
            (st) ->
            {
                st.raftHandler.onReplyVote(
                    FOLLOWER_1_ID, ID, LEADERSHIP_TERM, FOR, NODE_STATE_BUFFER, NODE_STATE_LENGTH, SESSION_ID);
                st.raftHandler.onReplyVote(
                    FOLLOWER_2_ID, ID, LEADERSHIP_TERM, FOR, NODE_STATE_BUFFER, NODE_STATE_LENGTH, SESSION_ID);
            }, "onMajority");

    private static Stimulus lowerPositionRequestVote =
        onRequestVote(CANDIDATE_ID, NEW_TERM, 0L, "lowerPositionRequestVote");

    private static Stimulus lowerTermRequestVote =
        onRequestVote(CANDIDATE_ID, LEADERSHIP_TERM, POSITION, "lowerTermRequestVote");

    private static Stimulus newLeaderRequestVote =
        onRequestVote(CANDIDATE_ID, NEW_TERM, POSITION, "newLeaderRequestVote");

    private static Stimulus onRequestVote(
        final short candidateId, final int leaderShipTerm, final long lastAckedPosition, final String name)
    {
        return namedStimulus(
            (st) -> st.raftHandler.onRequestVote(candidateId, SESSION_ID, leaderShipTerm, lastAckedPosition), name);
    }

    private void requestsVote(final int term)
    {
        verify(controlPublication, times(1)).saveRequestVote(
            eq(ID), anyInt(), eq(POSITION), eq(term));
    }

    private void setup()
    {
        when(dataSubscription.getImage(SESSION_ID)).thenReturn(leaderDataImage);
        when(archiver.session(SESSION_ID)).thenReturn(leaderArchiver);
        when(archiveReader.session(SESSION_ID)).thenReturn(sessionReader);

        termState.reset();
    }

    private static Object[] scenario(
        final RoleFixture roleFixture,
        final Stimulus stimulus,
        final Effect effect,
        final State state)
    {
        return new Object[]
        {
            roleFixture,
            stimulus,
            effect,
            state
        };
    }

    private static RoleFixture named(final RoleFixture fixture, final String name)
    {
        return new RoleFixture()
        {
            public Role apply(final ScenariosTest test)
            {
                return fixture.apply(test);
            }

            public String toString()
            {
                return name;
            }
        };
    }

    private static Stimulus namedStimulus(final Stimulus stimulus, final String name)
    {
        return new Stimulus()
        {
            public void accept(final ScenariosTest test)
            {
                stimulus.accept(test);
            }

            public String toString()
            {
                return name;
            }
        };
    }

    private static Effect namedEffect(final Effect effect, final String name)
    {
        return new Effect()
        {
            public void check(final ScenariosTest test)
            {
                effect.check(test);
            }

            public String toString()
            {
                return name;
            }
        };
    }

    private static State namedState(final State state, final String name)
    {
        return new State()
        {
            public void accept(final TermState termState)
            {
                state.accept(termState);
            }

            public String toString()
            {
                return name;
            }
        };
    }

    private static State ignored = namedState(
        (st) ->
        {
        }, "");
}
