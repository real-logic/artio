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

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.LogonEncoder;
import uk.co.real_logic.fix_gateway.messages.ReplayIndexRecordDecoder;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.*;
import static uk.co.real_logic.fix_gateway.engine.logger.ReplayIndex.logFile;

public class ReplayIndexTest extends AbstractLogTest
{
    private ByteBuffer indexBuffer = ByteBuffer.allocate(16 * 1024);
    private BufferFactory mockBufferFactory = mock(BufferFactory.class);

    private LogonEncoder logon = new LogonEncoder();
    private ReplayIndex replayIndex = new ReplayIndex(
        DEFAULT_LOG_FILE_DIR,
        DEFAULT_INDEX_FILE_SIZE,
        DEFAULT_LOGGER_CACHE_NUM_SETS,
        DEFAULT_LOGGER_CACHE_SET_SIZE,
        mockBufferFactory);

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
        assertEquals(AERON_SESSION_ID, replayIndexRecord.aeronSessionId());
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
        bufferContainsMessage(true, SESSION_ID, SEQUENCE_NUMBER);
        indexRecord();

        bufferContainsMessage(true, SESSION_ID_2, SEQUENCE_NUMBER);
        indexRecord();

        verifyMappedFile(SESSION_ID);
        verifyMappedFile(SESSION_ID_2);
    }

    @Test
    public void shouldIgnoreOtherMessageTypes()
    {
        bufferContainsLogon();

        indexRecord();

        verifyNoMoreInteractions(mockBufferFactory);
    }

    private void bufferContainsLogon()
    {
        offset = START;
        header
            .wrap(buffer, offset)
            .blockLength(logon.sbeBlockLength())
            .templateId(logon.sbeTemplateId())
            .schemaId(logon.sbeSchemaId())
            .version(logon.sbeSchemaVersion());

        offset += header.encodedLength();

        logon
            .wrap(buffer, offset)
            .connection(CONNECTION_ID)
            .session(SESSION_ID);

        offset += logon.encodedLength();
    }

    private void verifyMappedFile(final long sessionId)
    {
        verify(mockBufferFactory, times(1)).map(eq(logFile(DEFAULT_LOG_FILE_DIR, sessionId)), anyInt());
    }

    private void indexRecord()
    {
        replayIndex.indexRecord(buffer, START, fragmentLength(), STREAM_ID, AERON_SESSION_ID, START);
    }
}
