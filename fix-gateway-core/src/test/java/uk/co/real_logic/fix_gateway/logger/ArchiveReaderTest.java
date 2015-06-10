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
package uk.co.real_logic.fix_gateway.logger;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.ArchiveMetaDataDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.replication.ReplicationStreams;

import java.nio.ByteBuffer;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.fix_gateway.StaticConfiguration.LOGGER_CACHE_CAPACITY_DEFAULT;
import static uk.co.real_logic.fix_gateway.StaticConfiguration.LOG_FILE_DIR_DEFAULT;
import static uk.co.real_logic.fix_gateway.messages.FixMessageDecoder.BLOCK_LENGTH;

public class ArchiveReaderTest
{

    private static final byte DATA = (byte) 4;
    private static final int POSITION = 1;
    private static final int DATA_POSITION = HEADER_LENGTH + POSITION;
    private static final int LENGTH = 100;
    private static final int STREAM_ID = 1;
    public static final int SBE_BLOCK = MessageHeaderDecoder.SIZE + BLOCK_LENGTH;

    private ByteBuffer byteBuffer = ByteBuffer.allocate(16 * 1024);
    private UnsafeBuffer inputBuffer = new UnsafeBuffer(new byte[16 * 1024]);

    private DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
    private Header mockHeader = new Header(0, 0);
    private ReplicationStreams mockStreams = mock(ReplicationStreams.class);
    private LogHandler mockHandler = mock(LogHandler.class);
    private ArchiveMetaData mockMetaData = mock(ArchiveMetaData.class);
    private ArchiveMetaDataDecoder mockMetaDataDecoder = mock(ArchiveMetaDataDecoder.class);

    private Archiver archiver = new Archiver((file, size) -> byteBuffer, mockStreams, mockMetaData,
        LOG_FILE_DIR_DEFAULT, LOGGER_CACHE_CAPACITY_DEFAULT);

    private ArchiveReader archiveReader = new ArchiveReader(file -> byteBuffer, mockMetaData,
        LOG_FILE_DIR_DEFAULT, LOGGER_CACHE_CAPACITY_DEFAULT);

    @Before
    public void setUp()
    {
        mockHeader.buffer(inputBuffer);

        headerFlyweight.wrap(inputBuffer, POSITION);
        headerFlyweight.frameLength(LENGTH);

        inputBuffer.putByte(DATA_POSITION, DATA);

        when(mockMetaData.read(anyInt())).thenReturn(mockMetaDataDecoder);
    }

    @Test
    public void shouldReadStoredRecord()
    {
        dataStored();

        archiveReader.read(STREAM_ID, POSITION, mockHandler);

        verify(mockHandler).onLogEntry(
            notNull(FixMessageDecoder.class),
            notNull(UnsafeBuffer.class),
            eq(DATA_POSITION),
            eq(DATA_POSITION + SBE_BLOCK),
            eq(LENGTH - (SBE_BLOCK + HEADER_LENGTH)));
    }

    private void dataStored()
    {
        archiver.onFragment(inputBuffer, DATA_POSITION, LENGTH, mockHeader);
    }
}
