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
import uk.co.real_logic.fix_gateway.replication.LeadershipTerms.Cursor;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class LeadershipTermsTest
{

    private Cursor cursor = new Cursor();
    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[8 * 1024]);
    private LeadershipTerms leadershipTerms = new LeadershipTerms(buffer);

    @Test
    public void shouldFindNothingWithNoTerms()
    {
        findsNoTerm(1);
    }

    @Test
    public void shouldFindSingleTerm()
    {
        leadershipTerms.onNewLeader(0, 0, 0, 1);

        assertFindsPosition(10);

        assertCursor(1, 10);
    }

    @Test
    public void shouldFindPositionInTwoTerms()
    {
        leadershipTerms.onNewLeader(0, 0, 0, 1);
        leadershipTerms.onNewLeader(5, 5, 0, 2);

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
        // TODO
    }

    private void findsNoTerm(final int position)
    {
        assertFalse("Found term when there was none", leadershipTerms.find(position, cursor));
    }

    private void storesHistory()
    {
        leadershipTerms.onNewLeader(0, 0, 0, 1);
        leadershipTerms.onNewLeader(5, 5, 0, 2);
        leadershipTerms.onNewLeader(10, 15, 5, 1);
        leadershipTerms.onNewLeader(7, 17, 10, 2);
        leadershipTerms.onNewLeader(12, 19, 7, 1);
    }

    private void assertCursor(final int expectedSessionId, final int expectedStreamPosition)
    {
        assertEquals(expectedSessionId, cursor.sessionId());
        assertEquals(expectedStreamPosition, cursor.streamPosition());
    }

    private void assertFindsPosition(final int position)
    {
        assertTrue("Unable to find term", leadershipTerms.find(position, cursor));
    }
}
