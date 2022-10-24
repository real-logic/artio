package uk.co.real_logic.artio.system_tests;

import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import org.agrona.IoUtil;
import org.hamcrest.Matcher;
import uk.co.real_logic.artio.engine.FixEngine;

import java.io.File;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.TestFixtures.aeronArchiveContext;

public class Backup
{
    private final File backupLocation = new File("backup");

    public void cleanup()
    {
        if (backupLocation.exists())
        {
            IoUtil.delete(backupLocation, false);
        }
    }

    public void resetState(final FixEngine engine)
    {
        engine.resetState(backupLocation);
    }

    public void assertStateReset(
        final ArchivingMediaDriver mediaDriver, final Matcher<Integer> expectedNumberOfRecordings)
    {
        assertTrue("backupLocation missing", backupLocation.exists());
        assertTrue("backupLocation not directory", backupLocation.isDirectory());

        assertRecordingsDeleted(mediaDriver, expectedNumberOfRecordings);
    }

    private void assertRecordingsDeleted(
        final ArchivingMediaDriver mediaDriver, final Matcher<Integer> expectedNumberOfRecordings)
    {
        final int numberOfRecordings = getNumberOfRecordings(mediaDriver);
        assertThat(numberOfRecordings, expectedNumberOfRecordings);
    }

    int getNumberOfRecordings(final ArchivingMediaDriver mediaDriver)
    {
        final File archiveDir = mediaDriver.archive().context().archiveDir();
        final File[] recordings = archiveDir.listFiles(file -> file.getName().endsWith(".rec"));
        return Objects.requireNonNull(recordings).length;
    }

    public void assertRecordingsTruncated()
    {
        try (AeronArchive archive = AeronArchive.connect(aeronArchiveContext()))
        {
            archive.listRecording(0,
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
                    assertEquals(0, stopPosition);
                });
        }
    }
}
