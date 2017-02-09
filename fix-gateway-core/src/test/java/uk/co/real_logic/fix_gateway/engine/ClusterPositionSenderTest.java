/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine;

import io.aeron.Subscription;
import org.junit.After;
import org.junit.Test;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.replication.ClusterableSubscription;

import java.util.stream.IntStream;

import static io.aeron.Publication.BACK_PRESSURED;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.DEFAULT_NAME_PREFIX;

public class ClusterPositionSenderTest
{

    private static final int AERON_SESSION_ID = 1;
    private static final int LIBRARY_ID = 3;

    private static final int LENGTH = 64;
    private static long position(final int messageNumber)
    {
        return messageNumber * LENGTH;
    }

    private GatewayPublication publication = mock(GatewayPublication.class);
    private ClusterPositionSender positionSender = new ClusterPositionSender(
        mock(Subscription.class),
        mock(ClusterableSubscription.class),
        publication,
        DEFAULT_NAME_PREFIX);

    @Test
    public void shouldPublishPositionOfOnlyArchivedStream()
    {
        connectLibrary();
        checkConditions();
        onArchivedPosition(position(1), LENGTH);
        checkConditions();

        savedPosition(position(1));
    }

    @Test
    public void shouldPublishPositionOfOnlyArchivedStreamWhenBackPressured()
    {
        backPressureSave();

        connectLibrary();
        checkConditions();
        onArchivedPosition(position(1), LENGTH);
        checkConditions();
        checkConditions();

        savedPosition(position(1), times(2));
    }

    @Test
    public void shouldPublishPositionOfOnlyArchivedStreamOutOfOrder()
    {
        onArchivedPosition(position(1), LENGTH);
        checkConditions();
        connectLibrary();
        checkConditions();

        savedPosition(position(1));
    }

    @Test
    public void shouldPublishContiguousPositionOfReplicatedAndArchivedStream()
    {
        connectLibrary();
        onClusteredPosition(position(2), LENGTH);
        onArchivedPosition(position(1), LENGTH);
        checkConditions();

        savedPosition(position(2));
    }

    @Test
    public void shouldPublishContiguousPositionOfArchivedAndReplicatedStream()
    {
        contiguousStreamUpTo2();
    }

    private void contiguousStreamUpTo2()
    {
        connectLibrary();

        onClusteredPosition(position(1), LENGTH);
        onArchivedPosition(position(2), LENGTH);
        checkConditions();

        savedPosition(position(2));
    }

    @Test
    public void shouldNotPublishPositionWithGapIn()
    {
        contiguousStreamUpTo2();

        onArchivedPosition(position(4), LENGTH);
        checkConditions();

        savedPosition(position(2));
    }

    @Test
    public void shouldPublishPositionOnceGapFilled()
    {
        shouldNotPublishPositionWithGapIn();

        onClusteredPosition(position(3), LENGTH);
        checkConditions();

        savedPosition(position(4));
    }

    @Test
    public void shouldPublishPositionOnceMultipleGapsFilled()
    {
        contiguousStreamUpTo2();

        onArchivedPosition(position(4), LENGTH);
        onArchivedPosition(position(6), LENGTH);
        onArchivedPosition(position(8), LENGTH);
        checkConditions();

        onClusteredPosition(position(3), LENGTH);
        onClusteredPosition(position(5), LENGTH);
        onClusteredPosition(position(7), LENGTH);
        checkConditions();

        savedPosition(position(8));
    }

    @Test
    public void shouldPublishPositionWithIndividualMessagesFillingInALargeGap()
    {
        contiguousStreamUpTo2();

        onArchivedPosition(position(6), LENGTH);
        onArchivedPosition(position(7), LENGTH);
        onArchivedPosition(position(8), LENGTH);
        checkConditions();

        onArchivedPosition(position(3), LENGTH);
        onArchivedPosition(position(4), LENGTH);
        onArchivedPosition(position(5), LENGTH);
        checkConditions();

        savedPosition(position(8));
    }

    @Test
    public void shouldPublishPositionAfterWrapAround()
    {
        connectLibrary();

        IntStream.range(0, ClusterPositionSender.DEFAULT_INTERVAL_COUNT)
                 .forEach(x -> filledTwoSlotGap(x * 4 + 1));
    }

    // TODO: read not initially 0

    @Test
    public void shouldPublishPositionWithArbitraryNumberOfIntervals()
    {
        connectLibrary();

        final int counts = ClusterPositionSender.DEFAULT_INTERVAL_COUNT * 4;

        IntStream.rangeClosed(0, counts).forEach(x -> onArchivedPosition(position(x * 2 + 1), LENGTH));
        IntStream.rangeClosed(1, counts).forEach(x -> onArchivedPosition(position(x * 2), LENGTH));

        checkConditions();

        savedPosition(position(counts * 2 + 1));
    }

    @Test
    public void shouldPublishPositionWithArbitraryNumberOfIntervalsAtArbitraryOffset()
    {
        connectLibrary();

        // Ensure that the read pointed is > 0 when you copy
        onArchivedPosition(position(1), LENGTH);
        onClusteredPosition(position(3), LENGTH);
        onArchivedPosition(position(2), LENGTH);
        onClusteredPosition(position(4), LENGTH);
        checkConditions();
        savedPosition(position(4));

        final int counts = ClusterPositionSender.DEFAULT_INTERVAL_COUNT * 4;

        IntStream.rangeClosed(2, counts).forEach(x -> onArchivedPosition(position(x * 2 + 1), LENGTH));
        IntStream.rangeClosed(3, counts).forEach(x -> onArchivedPosition(position(x * 2), LENGTH));

        checkConditions();

        savedPosition(position(counts * 2 + 1));
    }

    @Test
    public void shouldRemoveRecordUponTimeout()
    {
        libraryAtPosition1();

        onLibraryTimeout();

        onArchivedPosition(position(2), LENGTH);
        checkConditions();
    }

    // NB: Framer uses heartbeat's for re-enabling liveness so the this should as well to have a
    // Consistent view of Library liveness
    @Test
    public void shouldMakeLibraryLiveUponHeartbeat()
    {
        libraryAtPosition1();

        onLibraryTimeout();
        positionSender.onApplicationHeartbeat(AERON_SESSION_ID, LIBRARY_ID);

        onArchivedPosition(position(2), LENGTH);
        checkConditions();

        savedPosition(position(2));
    }

    @After
    public void noMorePublications()
    {
        verifyNoMoreInteractions(publication);
    }

    private void libraryAtPosition1()
    {
        connectLibrary();
        onArchivedPosition(position(1), LENGTH);
        checkConditions();
        savedPosition(position(1));
    }

    private void onLibraryTimeout()
    {
        positionSender.onLibraryTimeout(AERON_SESSION_ID, LIBRARY_ID);
    }

    private void filledTwoSlotGap(final int startingAt)
    {
        onArchivedPosition(position(startingAt + 1), LENGTH);
        onArchivedPosition(position(startingAt + 3), LENGTH);

        onClusteredPosition(position(startingAt), LENGTH);
        onClusteredPosition(position(startingAt + 2), LENGTH);
        checkConditions();

        savedPosition(position(startingAt + 3));
    }

    private void backPressureSave()
    {
        when(publication.saveNewSentPosition(anyInt(), anyLong()))
            .thenReturn(BACK_PRESSURED, 1024L);
    }

    private void checkConditions()
    {
        positionSender.checkConditions();
    }

    private void savedPosition(final long position)
    {
        savedPosition(position, times(1));
    }

    private void savedPosition(final long position, final VerificationMode mode)
    {
        verify(publication, mode).saveNewSentPosition(LIBRARY_ID, position);
    }

    private void connectLibrary()
    {
        positionSender.onLibraryConnect(AERON_SESSION_ID, LIBRARY_ID);
    }

    private void onArchivedPosition(final long position, final int length)
    {
        positionSender.onArchivedPosition(AERON_SESSION_ID, position, length);
    }

    private void onClusteredPosition(final long position, final int length)
    {
        positionSender.onClusteredLibraryPosition(LIBRARY_ID, position, length);
    }
}
