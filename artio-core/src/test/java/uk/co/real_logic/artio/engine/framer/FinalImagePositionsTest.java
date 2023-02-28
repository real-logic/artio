/*
 * Copyright 2015-2023 Real Logic Limited.
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

import io.aeron.Image;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.real_logic.artio.engine.framer.FinalImagePositions.UNKNOWN_POSITION;

public class FinalImagePositionsTest
{
    private static final int SESSION_ID = 1;
    private static final long POSITION = 1024;

    private final Image image = mock(Image.class);
    private final FinalImagePositions finalImagePositions = new FinalImagePositions();

    @Test
    public void shouldReturnUnknownIfNotNotified()
    {
        assertPositionIs(UNKNOWN_POSITION);
    }

    @Test
    public void shouldReturnPositionIfPresent()
    {
        savePosition();

        assertPositionIs(POSITION);
    }

    @Test
    public void shouldReadPositionMultipleTimesBeforeRemoval()
    {
        savePosition();

        assertPositionIs(POSITION);

        assertPositionIs(POSITION);

        assertPositionIs(POSITION);

        removePosition();

        assertPositionIs(UNKNOWN_POSITION);
    }

    @Test
    public void shouldNotLeakMemoryForPositionIfAlreadyRemoved()
    {
        removePosition();

        savePosition();

        assertPositionIs(UNKNOWN_POSITION);
    }

    private void savePosition()
    {
        when(image.position()).thenReturn(POSITION);
        when(image.sessionId()).thenReturn(SESSION_ID);

        finalImagePositions.onUnavailableImage(image);
    }

    private void assertPositionIs(final long expectedPosition)
    {
        assertEquals(expectedPosition, finalImagePositions.lookupPosition(SESSION_ID));
    }

    private void removePosition()
    {
        finalImagePositions.removePosition(SESSION_ID);
    }

}
