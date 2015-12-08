/*
 * Copyright 2015 Real Logic Ltd.
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;
import static uk.co.real_logic.fix_gateway.replication.Follower.NO_ONE;
import static uk.co.real_logic.fix_gateway.replication.ReplicationAsserts.*;

@RunWith(Parameterized.class)
public class ScenariosTest
{
    private static final short ID = 2;
    private static final int OLD_TERM = 0;
    private static final int LEADERSHIP_TERM = OLD_TERM + 1;
    private static final int NEW_TERM = LEADERSHIP_TERM + 1;
    private static final int LEADER_SESSION_ID = 42;
    private static final long TIME = 10L;
    private static final long POSITION = 40L;
    private static final int HEARTBEAT_INTERVAL_IN_MS = 10;
    private static final short NEW_LEADER_ID = 3;
    private static final short FOLLOWER_ID = 4;
    private static final short CANDIDATE_ID = 5;
    private static final int NEW_LEADER_SESSION_ID = 43;

    private final RaftPublication controlPublication = mock(RaftPublication.class);
    private final RaftNode raftNode = mock(RaftNode.class);
    private final Subscription acknowledgementSubscription = mock(Subscription.class);
    private final Subscription dataSubscription = mock(Subscription.class);
    private final Image leaderDataImage = mock(Image.class);
    private final ArchiveReader archiveReader = mock(ArchiveReader.class);
    private final TermState termState = new TermState();
    private final FragmentHandler fragmentHandler = mock(FragmentHandler.class);

    private final RoleFixture roleFixture;
    private final Stimulus stimulus;
    private final Effect requiredEffect;
    private final State requiredState;

    private Role role;
    private RaftHandler raftHandler;

    @Parameterized.Parameters(name = "'{'{0}'}' {1} '{'{2}, {3}'}'")
    public static Iterable<Object[]> parameters()
    {
        return Arrays.<Object[]>asList(
            scenario(
                leader,
                receivesHeartbeat(NEW_LEADER_ID, NEW_TERM, NEW_LEADER_SESSION_ID, "newLeaderHeartbeat"),
                transitionsToFollower,
                hasNewLeader),

            scenario(
                leader,
                receivesHeartbeat(NEW_LEADER_ID, OLD_TERM, NEW_LEADER_SESSION_ID, "oldTermLeaderHeartbeat"),
                neverTransitionsToFollower,
                ignored),

            scenario(
                leader,
                receivesHeartbeat(ID, NEW_TERM, LEADER_SESSION_ID, "selfHeartbeat"),
                neverTransitionsToFollower,
                ignored),

            scenario(
                leader,
                onRequestVote(CANDIDATE_ID, NEW_TERM, POSITION, "newLeaderRequestVote"),
                voteForCandidate.and(transitionsToFollowerOf(CANDIDATE_ID)),
                hasNoLeader),

            scenario(
                leader,
                onRequestVote(CANDIDATE_ID, LEADERSHIP_TERM, POSITION, "lowerTermRequestVote"),
                neverTransitionsToFollower,
                ignored),

            scenario(
                leader,
                onRequestVote(CANDIDATE_ID, NEW_TERM, 0L, "lowerPositionRequestVote"),
                neverTransitionsToFollower,
                ignored)
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
        raftHandler = (RaftHandler) role;

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

    private static State hasNewLeader =
        named(termState ->
        {
            assertThat(termState, hasLeaderSessionId(NEW_LEADER_SESSION_ID));
            assertThat(termState, hasLeadershipTerm(NEW_TERM));
            assertThat(termState, hasPositions(POSITION));
        }, "hasNewLeader");

    private static State hasNoLeader =
        named(termState ->
        {
            assertThat(termState, hasNoLeader());
            assertThat(termState, hasLeadershipTerm(NEW_TERM));
            assertThat(termState, hasPositions(POSITION));
        }, "hasNoLeader");

    private static RoleFixture leader = named(ScenariosTest::leader, "leader");

    private Role leader()
    {
        termState.leadershipTerm(LEADERSHIP_TERM).commitPosition(POSITION);

        final Leader leader = new Leader(
            ID,
            new EntireClusterAcknowledgementStrategy(),
            new IntHashSet(40, -1),
            raftNode,
            mock(FragmentHandler.class),
            0,
            HEARTBEAT_INTERVAL_IN_MS,
            termState,
            LEADER_SESSION_ID,
            archiveReader);

        return leader
            .controlPublication(controlPublication)
            .acknowledgementSubscription(acknowledgementSubscription)
            .dataSubscription(dataSubscription)
            .getsElected(TIME);
    }

    private static Effect voteForCandidate = namedEffect(st ->
            verify(st.controlPublication).saveReplyVote(ID, CANDIDATE_ID, NEW_TERM, FOR), "voteForCandidate");

    private static Effect transitionsToFollower =
        transitionsToFollower(NO_ONE, "transitionsToFollower");

    private static Effect transitionsToFollowerOf(final int votedFor)
    {
        return transitionsToFollower(votedFor, "transitionsToFollowerOf" + votedFor);
    }

    private static Effect transitionsToFollower(final int votedFor, final String name)
    {
        return namedEffect(st ->
        {
            final Leader leader = (Leader) st.role;
            verify(st.raftNode, atLeastOnce()).transitionToFollower(eq(leader), eq(votedFor), anyLong());
        }, name);
    }

    private static Effect neverTransitionsToFollower =
        namedEffect(st -> ReplicationAsserts.neverTransitionsToFollower(st.raftNode), "neverTransitionsToFollower");

    private static Stimulus receivesHeartbeat(final short leaderId,
                                              final int leaderShipTerm,
                                              final int dataSessionId,
                                              final String name)
    {
        return namedStimulus(st ->
        {
            st.raftHandler.onConcensusHeartbeat(leaderId, leaderShipTerm, POSITION, dataSessionId);
        }, name);
    }

    private static Stimulus onRequestVote(
        final short candidateId, final int leaderShipTerm, final long lastAckedPosition, final String name)
    {
        return namedStimulus(st ->
        {
            st.raftHandler.onRequestVote(candidateId, leaderShipTerm, lastAckedPosition);
        }, name);
    }

    private void setup()
    {
        when(dataSubscription.getImage(LEADER_SESSION_ID)).thenReturn(leaderDataImage);
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

    private static State named(final State state, final String name)
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

    private static State ignored = named(st -> { } , "");

}
