package uk.co.real_logic.fix_gateway.logger;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.ArchiveMetaDataDecoder;
import uk.co.real_logic.fix_gateway.messages.ArchiveMetaDataEncoder;

import java.io.File;

import static uk.co.real_logic.agrona.IoUtil.mapExistingFile;
import static uk.co.real_logic.agrona.IoUtil.mapNewFile;

// TODO: add a message header to the archive format
// TODO: unmap mapped byte buffers
public class ArchiveMetaData
{
    private final ArchiveMetaDataDecoder decoder = new ArchiveMetaDataDecoder();
    private final ArchiveMetaDataEncoder encoder = new ArchiveMetaDataEncoder();
    private final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(0, encoder.sbeBlockLength());

    public ArchiveMetaData()
    {
        encoder.wrap(metaDataBuffer, 0);
        decoder.wrap(metaDataBuffer, 0, encoder.sbeBlockLength(), encoder.sbeSchemaVersion());
    }

    public void write(final int streamId, final int initialTermId)
    {
        final File metaDataFile = metaDataFile(streamId);
        if (!metaDataFile.exists())
        {
            metaDataBuffer.wrap(mapNewFile(metaDataFile, encoder.sbeBlockLength()));
            encoder.initialTermId(initialTermId);
        }
    }

    public ArchiveMetaDataDecoder read(final int streamId)
    {
        final File metaDataFile = metaDataFile(streamId);
        metaDataBuffer.wrap(mapExistingFile(metaDataFile, "metaDataFile"));
        return decoder;
    }

    private File metaDataFile(int streamId)
    {
        return new File(LogDirectoryDescriptor.metaDatalogFile(streamId));
    }
}
