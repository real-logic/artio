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

    private static final long POSITION = 1042;
    private static final long NEXT_POSITION = POSITION + 100;
    private static final int ALIGNED_LENGTH = 64;

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
        onArchivedPosition(POSITION);
        checkConditions();

        savedPosition(POSITION);
    }

    @Test
    public void shouldPublishPositionOfOnlyArchivedStreamWhenBackPressured()
    {
        backPressureSave();

        connectLibrary();
        checkConditions();
        onArchivedPosition(POSITION);
        checkConditions();

        savedPosition(POSITION, times(2));
    }

    @Test
    public void shouldPublishPositionOfOnlyArchivedStreamOutOfOrder()
    {
        onArchivedPosition(POSITION);
        checkConditions();
        connectLibrary();
        checkConditions();

        savedPosition(POSITION);
    }

    @Test
    public void shouldPublishMinimumPositionOfReplicatedAndArchivedStream()
    {
        connectLibrary();
        onClusteredPosition(NEXT_POSITION);
        onArchivedPosition(POSITION);
        checkConditions();

        savedPosition(POSITION);
    }

    @Test
    public void shouldPublishMinimumPositionOfArchivedAndReplicatedStream()
    {
        connectLibrary();
        onClusteredPosition(POSITION);
        onArchivedPosition(NEXT_POSITION);
        checkConditions();

        savedPosition(POSITION);
    }

    @Test
    public void shouldPublishMinimumPositionOfArchivedAndReplicatedStreamOutOfOrder()
    {
        onClusteredPosition(POSITION);
        onArchivedPosition(NEXT_POSITION);
        checkConditions();

        connectLibrary();
        checkConditions();

        savedPosition(POSITION);
    }

    @Test
    public void shouldOnlyUpdatePositionWhenArchivedAndReplicatedPositionsHaveReachedIt()
    {
        shouldPublishMinimumPositionOfReplicatedAndArchivedStream();

        onArchivedPosition(NEXT_POSITION);
        checkConditions();

        savedPosition(NEXT_POSITION);
    }

    @Test
    public void shouldNotPublishPositionOfNotArchivedStream()
    {
        connectLibrary();
        onClusteredPosition(POSITION);
        checkConditions();

        notSavedPosition();
    }

    private void backPressureSave()
    {
        when(publication.saveNewSentPosition(anyInt(), anyLong()))
            .thenReturn(BACK_PRESSURED, 1024L);
    }

    private void notSavedPosition()
    {
        verify(publication, never()).saveNewSentPosition(anyInt(), anyLong());
    }

    private void checkConditions()
    {
        positionSender.checkConditions();
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

    private void onArchivedPosition(final long position)
    {
        positionSender.onArchivedPosition(AERON_SESSION_ID, position, ALIGNED_LENGTH);
    }

    private void onClusteredPosition(final long position)
    {
        positionSender.onClusteredLibraryPosition(LIBRARY_ID, position, ALIGNED_LENGTH);
    }

    // TODO: sustained non-replicated messages without a replicated message update

}
