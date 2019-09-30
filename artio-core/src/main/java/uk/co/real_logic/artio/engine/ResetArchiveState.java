/*
 * Copyright 2019 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.collections.LongHashSet;

import java.io.File;

class ResetArchiveState
{
    private final File backupLocation;
    private final EngineConfiguration configuration;

    private AeronArchive archive;
    private Aeron aeron;

    ResetArchiveState(final EngineConfiguration configuration, final File backupLocation)
    {
        this.configuration = configuration;
        this.backupLocation = backupLocation;
    }

    void resetState()
    {
        if (configuration.logAnyMessages())
        {
            createArchiver();
            try
            {
                truncateArchive();
                backupState();
            }
            finally
            {
                CloseHelper.close(archive);
                CloseHelper.close(aeron);
            }
        }
    }

    private void truncateArchive()
    {
        final String channel = configuration.libraryAeronChannel();
        final LongHashSet relevantRecordingIds = new LongHashSet();
        archive.listRecordings(0,
            Integer.MAX_VALUE,
            (controlSessionId,
            correlationId,
            recordingId,
            startTimestamp,
            stopTimestamp,
            startPosition,
            stopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) ->
            {
                if (configuration.isRelevantStreamId(streamId) && originalChannel.equals(channel))
                {
                    relevantRecordingIds.add(recordingId);
                }
            });

        for (final long recordingId : relevantRecordingIds)
        {
            archive.truncateRecording(recordingId, 0);
        }
    }

    private void createArchiver()
    {
        aeron = Aeron.connect(configuration.aeronContextClone());
        final AeronArchive.Context archiveContext = configuration.archiveContextClone();
        archive = AeronArchive.connect(archiveContext.aeron(aeron));
    }

    private void backupState()
    {
        if (backupLocation != null)
        {
            final File backupDir = backupLocation.getAbsoluteFile();

            if (backupLocation.exists())
            {
                if (!backupLocation.isDirectory())
                {
                    throw new IllegalStateException(backupDir + " is not a directory, so backup cannot proceed");
                }
            }
            else if (!backupLocation.mkdirs())
            {
                throw new IllegalStateException(backupDir + " could not be created, so backup cannot proceed");
            }

            final File logFileDir = new File(configuration.logFileDir());
            for (final File file : logFileDir.listFiles())
            {
                if (!file.renameTo(new File(backupDir, file.getName())))
                {
                    throw new IllegalStateException(
                        "Unable to move " + file.getAbsolutePath() + " to " + backupDir);
                }
            }
        }
    }
}
