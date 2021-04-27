/*
 * Copyright 2020 Monotonic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.engine.framer;

import org.junit.Test;

import java.util.Arrays;
import java.util.function.LongConsumer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.real_logic.artio.engine.framer.ReceiverEndPoints.disconnectILinkConnections;

public class ReceiverEndPointsTest
{
    private static final int LIBRARY_ID = 1;

    private final LongConsumer removeFunc = mock(LongConsumer.class);

    @Test
    public void shouldRemoveILink3EndPoints()
    {
        final ReceiverEndPoint[] endPoints = makeEndPoints();
        final ReceiverEndPoint[] expected = { endPoints[0], endPoints[2], endPoints[3] };

        isOwned(endPoints, 1);
        isOwned(endPoints, 4);

        final ReceiverEndPoint[] result = disconnectILinkConnections(LIBRARY_ID, endPoints, removeFunc);

        assertArrayEquals(Arrays.toString(result), expected, result);
    }

    @Test
    public void shouldRemoveILink3EndPoint()
    {
        final ReceiverEndPoint[] endPoints = { mock(InitiatorFixPReceiverEndPoint.class) };
        final ReceiverEndPoint[] expected = { };

        isOwned(endPoints, 0);

        final ReceiverEndPoint[] result = disconnectILinkConnections(LIBRARY_ID, endPoints, removeFunc);

        assertArrayEquals(Arrays.toString(result), expected, result);
    }

    @Test
    public void shouldNotRemoveIrrelevantEndPoints()
    {
        final ReceiverEndPoint[] endPoints = makeEndPoints();

        isOwned(endPoints, 2);

        final ReceiverEndPoint[] result = disconnectILinkConnections(LIBRARY_ID, endPoints, removeFunc);
        assertSame(endPoints, result);
    }

    private ReceiverEndPoint[] makeEndPoints()
    {
        final ReceiverEndPoint[] endPoints = new ReceiverEndPoint[5];
        endPoints[0] = mock(FixReceiverEndPoint.class);
        endPoints[1] = mock(InitiatorFixPReceiverEndPoint.class);
        endPoints[2] = mock(FixReceiverEndPoint.class);
        endPoints[3] = mock(InitiatorFixPReceiverEndPoint.class);
        endPoints[4] = mock(InitiatorFixPReceiverEndPoint.class);
        return endPoints;
    }

    private void isOwned(final ReceiverEndPoint[] endPoints, final int i)
    {
        when(endPoints[i].libraryId()).thenReturn(LIBRARY_ID);
    }
}
