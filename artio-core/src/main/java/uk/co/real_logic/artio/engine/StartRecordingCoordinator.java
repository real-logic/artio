/*
 * Copyright 2015-2018 Real Logic Ltd.
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
package uk.co.real_logic.artio.engine;

import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.IntHashSet.IntIterator;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.archive.codecs.SourceLocation.REMOTE;
import static uk.co.real_logic.artio.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.artio.GatewayProcess.OUTBOUND_LIBRARY_STREAM;

public class StartRecordingCoordinator
{
    private final IntHashSet trackedSessionIds = new IntHashSet();
    private final AeronArchive archive;

    StartRecordingCoordinator(final AeronArchive archive, final String channel)
    {
        this.archive = archive;

        // Inbound we're writing from the Framer thread, always local
        archive.startRecording(channel, INBOUND_LIBRARY_STREAM, LOCAL);

        // Outbound libraries might be on an IPC box.
        final SourceLocation location = channel.equals(IPC_CHANNEL) ? LOCAL : REMOTE;
        archive.startRecording(channel, OUTBOUND_LIBRARY_STREAM, location);
    }

    public void track(final Publication publication)
    {
        trackedSessionIds.add(publication.sessionId());
    }

    void awaitReady()
    {
        final CountersReader counters = archive.context().aeron().countersReader();
        while (!trackedSessionIds.isEmpty())
        {
            final IntIterator sessionIdIterator = trackedSessionIds.iterator();
            while (sessionIdIterator.hasNext())
            {
                final int sessionId = sessionIdIterator.nextValue();
                if (hasRecordingStarted(counters, sessionId))
                {
                    sessionIdIterator.remove();
                }
            }

            Thread.yield();
        }
    }

    private boolean hasRecordingStarted(final CountersReader counters, final int sessionId)
    {
        return RecordingPos.findCounterIdBySession(counters, sessionId) != CountersReader.NULL_COUNTER_ID;
    }
}
