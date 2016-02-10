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
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.replication.LeadershipTermIndex.Cursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LeadershipTermIndexTest
{
    private final Cursor cursor = new Cursor();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[8 * 1024]);
    private final LeadershipTermIndex leadershipTermIndex = new LeadershipTermIndex(buffer);

    @Test
    public void shouldFindNothingWithNoTerms()
    {
        findsNoTerm(1);
    }

    @Test
    public void shouldFindSingleTerm()
    {
        leadershipTermIndex.onNewLeader(0, 0, 0, 1);

        assertFindsPosition(10);

        assertCursor(1, 10);
    }

    @Test
    public void shouldFindPositionInTwoTerms()
    {
        leadershipTermIndex.onNewLeader(0, 0, 0, 1);
        leadershipTermIndex.onNewLeader(5, 5, 0, 2);

        assertFindsPosition(10);

        assertCursor(2, 5);
    }

    @Test
    public void shouldSearchStore()
    {
        storesHistory();

        assertFindsPosition(18);

        assertCursor(2, 11);
    }

    @Test
    public void shouldFindUpperPosition()
    {
        storesHistory();

        assertFindsPosition(30);

        assertCursor(1, 18);
    }

    @Test
    public void shouldFindLowerPosition()
    {
        storesHistory();

        assertFindsPosition(1);

        assertCursor(1, 1);
    }

    @Test
    public void shouldFindNothingWhenOutOfLowerBound()
    {
        storesHistory();

        findsNoTerm(-1);
    }

    @Test
    public void shouldLoadPreviouslySavedTerms()
    {
        storesHistory();

        final LeadershipTermIndex newLeadershipTermIndex = new LeadershipTermIndex(buffer);

        assertTrue("Unable to find term", newLeadershipTermIndex.find(18, cursor));

        assertCursor(2, 11);
    }

    private void findsNoTerm(final int position)
    {
        assertFalse("Found term when there was none", leadershipTermIndex.find(position, cursor));
    }

    private void storesHistory()
    {
        leadershipTermIndex.onNewLeader(0, 0, 0, 1);
        leadershipTermIndex.onNewLeader(5, 5, 0, 2);
        leadershipTermIndex.onNewLeader(10, 15, 5, 1);
        leadershipTermIndex.onNewLeader(7, 17, 10, 2);
        leadershipTermIndex.onNewLeader(12, 19, 7, 1);
    }

    private void assertCursor(final int expectedSessionId, final int expectedStreamPosition)
    {
        assertEquals(expectedSessionId, cursor.sessionId());
        assertEquals(expectedStreamPosition, cursor.streamPosition());
    }

    private void assertFindsPosition(final int position)
    {
        assertTrue("Unable to find term", leadershipTermIndex.find(position, cursor));
    }
}
