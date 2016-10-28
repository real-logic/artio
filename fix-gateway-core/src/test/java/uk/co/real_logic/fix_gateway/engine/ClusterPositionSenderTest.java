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

import org.junit.After;
import org.junit.Test;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.replication.ClusterableSubscription;

import static io.aeron.Publication.BACK_PRESSURED;
import static org.mockito.Mockito.*;

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
        mock(ClusterableSubscription.class),
        mock(ClusterableSubscription.class),
        publication);

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

        savedPosition(position(2));
    }

    private void contiguousStreamUpTo2()
    {
        connectLibrary();
        onClusteredPosition(position(1), LENGTH);
        onArchivedPosition(position(2), LENGTH);
        checkConditions();
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

    @After
    public void noMorePublications()
    {
        verifyNoMoreInteractions(publication);
    }

    // TODO: multiple gaps filled in
    // TODO: gaps in archived position
    // TODO: don't leak memory when libraries disconnect.

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
