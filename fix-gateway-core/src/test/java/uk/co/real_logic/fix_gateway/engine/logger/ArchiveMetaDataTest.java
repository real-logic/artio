/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine.logger;

import org.agrona.IoUtil;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;
import uk.co.real_logic.fix_gateway.storage.messages.ArchiveMetaDataDecoder;

import java.io.File;
import java.io.IOException;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.junit.Assert.*;

public class ArchiveMetaDataTest
{
    public static final StreamIdentifier STREAM_ID = new StreamIdentifier(IPC_CHANNEL, 1);
    public static final int SESSION_ID = 2;
    public static final int INITIAL_TERM_ID = 12;
    public static final int TERM_BUFFER_LENGTH = 13;

    private String tempDir = IoUtil.tmpDirName() + File.separator + "amdt";
    private LogDirectoryDescriptor directory = new LogDirectoryDescriptor(tempDir);
    private ArchiveMetaData archiveMetaData = newArchiveMetaData();

    @After
    public void teardown()
    {
        archiveMetaData.close();
        final File dir = new File(tempDir);
        if (dir.exists())
        {
            IoUtil.delete(dir, false);
        }
    }

    @Test
    public void shouldValidateFileExists()
    {
        final ArchiveMetaDataDecoder decoder = archiveMetaData.read(STREAM_ID, SESSION_ID);
        assertNull(decoder);

        archiveMetaData.close();
    }

    @Test
    public void shouldValidateLengthOfBuffer() throws IOException
    {
        final File metaDataFile = directory.metaDataLogFile(STREAM_ID, SESSION_ID);
        IoUtil.mapNewFile(metaDataFile, 0);

        final ArchiveMetaDataDecoder decoder = archiveMetaData.read(STREAM_ID, SESSION_ID);
        assertNull(decoder);
    }

    @Test
    public void shouldStoreMetaDataInformation()
    {
        archiveMetaData.write(STREAM_ID, SESSION_ID, INITIAL_TERM_ID, TERM_BUFFER_LENGTH);

        final ArchiveMetaDataDecoder decoder = archiveMetaData.read(STREAM_ID, SESSION_ID);
        assertEquals(INITIAL_TERM_ID, decoder.initialTermId());
        assertEquals(TERM_BUFFER_LENGTH, decoder.termBufferLength());
    }

    private ArchiveMetaData newArchiveMetaData()
    {
        return new ArchiveMetaData(directory, LoggerUtil::mapExistingFile, IoUtil::mapNewFile);
    }

}
