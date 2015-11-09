/*
 * Copyright 2015 Real Logic Ltd.
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

import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.fix_gateway.messages.ArchiveMetaDataDecoder;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.aeron.CommonContext.IPC_CHANNEL;

public class ArchiveMetaDataTest
{
    public static final StreamIdentifier STREAM_ID = new StreamIdentifier(IPC_CHANNEL, 1);
    public static final int SESSION_ID = 2;
    public static final int INITIAL_TERM_ID = 12;
    public static final int TERM_BUFFER_LENGTH = 13;

    private ByteBuffer buffer = ByteBuffer.allocate(8 * 1024);
    private ExistingBufferFactory existingBufferFactory = LoggerUtil::mapExistingFile;
    private BufferFactory newBufferFactory = LoggerUtil::map;
    private String tempDir = IoUtil.tmpDirName() + File.separator + "amdt";
    private LogDirectoryDescriptor directory = new LogDirectoryDescriptor(tempDir);
    private ArchiveMetaData archiveMetaData = new ArchiveMetaData(directory, existingBufferFactory, newBufferFactory);

    @After
    public void teardown()
    {
        IoUtil.delete(new File(tempDir), true);
    }

    @Test
    public void shouldStoreMetaDataInformation()
    {
        archiveMetaData.write(STREAM_ID, SESSION_ID, INITIAL_TERM_ID, TERM_BUFFER_LENGTH);

        final ArchiveMetaDataDecoder decoder = archiveMetaData.read(STREAM_ID, SESSION_ID);
        assertEquals(INITIAL_TERM_ID, decoder.initialTermId());
        assertEquals(TERM_BUFFER_LENGTH, decoder.termBufferLength());
    }

}
