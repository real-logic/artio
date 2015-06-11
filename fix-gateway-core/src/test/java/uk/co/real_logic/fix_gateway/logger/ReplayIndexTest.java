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
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.ConnectEncoder;
import uk.co.real_logic.fix_gateway.messages.ReplayIndexRecordDecoder;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.StaticConfiguration.DEFAULT_INDEX_FILE_SIZE;
import static uk.co.real_logic.fix_gateway.StaticConfiguration.DEFAULT_LOGGER_CACHE_CAPACITY;
import static uk.co.real_logic.fix_gateway.StaticConfiguration.DEFAULT_LOG_FILE_DIR;
import static uk.co.real_logic.fix_gateway.logger.ReplayIndex.logFile;

public class ReplayIndexTest extends AbstractMessageTest
{

    private ByteBuffer indexBuffer = ByteBuffer.allocate(16 * 1024);
    private BufferFactory mockBufferFactory = mock(BufferFactory.class);

    private ConnectEncoder connect = new ConnectEncoder();
    private ReplayIndex replayIndex = new ReplayIndex(
        DEFAULT_LOG_FILE_DIR, DEFAULT_INDEX_FILE_SIZE, DEFAULT_LOGGER_CACHE_CAPACITY, mockBufferFactory);

    @Before
    public void setUp()
    {
        when(mockBufferFactory.map(any(File.class), anyInt())).thenReturn(indexBuffer);
    }

    @Test
    public void shouldRecordIndexEntryForFixMessage()
    {
        bufferContainsMessage(true);

        indexRecord();

        verifyMappedFile(SESSION_ID);

        final ReplayIndexRecordDecoder replayIndexRecord = new ReplayIndexRecordDecoder()
            .wrap(new UnsafeBuffer(indexBuffer), 8, 16, 0);

        assertEquals(STREAM_ID, replayIndexRecord.streamId());
        assertEquals(START, replayIndexRecord.position());
        assertEquals(SEQUENCE_NUMBER, replayIndexRecord.sequenceNumber());
    }

    @Test
    public void shouldOnlyMapSessionFileOnce()
    {
        bufferContainsMessage(true);

        indexRecord();
        indexRecord();

        verifyMappedFile(SESSION_ID);
    }

    @Test
    public void shouldRecordIndexesForMultipleSessions()
    {
        bufferContainsMessage(true, SESSION_ID);
        indexRecord();

        bufferContainsMessage(true, SESSION_ID_2);
        indexRecord();

        verifyMappedFile(SESSION_ID);
        verifyMappedFile(SESSION_ID_2);
    }

    @Test
    public void shouldIgnoreOtherMessageTypes()
    {
        bufferContainsConnect();

        indexRecord();

        verifyNoMoreInteractions(mockBufferFactory);
    }

    private void bufferContainsConnect()
    {
        offset = START;
        header
            .wrap(buffer, offset)
            .blockLength(connect.sbeBlockLength())
            .templateId(connect.sbeTemplateId())
            .schemaId(connect.sbeSchemaId())
            .version(connect.sbeSchemaVersion());

        offset += header.size();

        connect
            .wrap(buffer, offset)
            .connection(CONNECTION_ID)
            .session(SESSION_ID);

        offset += connect.size();
    }

    private void verifyMappedFile(final long sessionId)
    {
        verify(mockBufferFactory, times(1)).map(eq(logFile(DEFAULT_LOG_FILE_DIR, sessionId)), anyInt());
    }

    private void indexRecord()
    {
        replayIndex.indexRecord(buffer, START, messageLength(), STREAM_ID);
    }

}
