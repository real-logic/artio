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
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.replication.ReplicationStreams;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.fix_gateway.StaticConfiguration.DEFAULT_LOGGER_CACHE_CAPACITY;
import static uk.co.real_logic.fix_gateway.StaticConfiguration.DEFAULT_LOG_FILE_DIR;

public class ArchiverTest
{
    private static final byte DATA = (byte) 4;
    private static final int DATA_POSITION = HEADER_LENGTH + 1;
    private static final int LENGTH = 10;
    private static final int STREAM_ID = 1;

    private DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
    private Header mockHeader = new Header(0, 0);
    private ReplicationStreams mockStreams = mock(ReplicationStreams.class);
    private BufferFactory mockBufferFactory = mock(BufferFactory.class);
    private ArchiveMetaData mockMetaData = mock(ArchiveMetaData.class);

    private ByteBuffer byteBuffer = ByteBuffer.allocate(16 * 1024);
    private UnsafeBuffer outputBuffer = new UnsafeBuffer(byteBuffer);
    private UnsafeBuffer inputBuffer = new UnsafeBuffer(new byte[16 * 1024]);

    private Archiver archiver = new Archiver(mockBufferFactory, mockStreams, mockMetaData,
        DEFAULT_LOG_FILE_DIR, DEFAULT_LOGGER_CACHE_CAPACITY);

    @Before
    public void setUp()
    {
        mockHeader.buffer(inputBuffer);

        when(mockBufferFactory.map(any(File.class), anyInt())).thenReturn(byteBuffer);

        inputBuffer.putByte(DATA_POSITION, DATA);
    }

    private void setupHeader(final int termId, final int streamId)
    {
        headerFlyweight.wrap(inputBuffer, 0);
        headerFlyweight.streamId(streamId);
        headerFlyweight.termId(termId);
    }

    @Test
    public void shouldCopyDataToBuffer()
    {
        setupHeader(2, STREAM_ID);

        onData();

        verifyBufferMapped(1);
        assertEquals(4, outputBuffer.getByte(DATA_POSITION));
    }

    @Test
    public void shouldRotateBuffersGivenNewTermId()
    {
        setupHeader(2, STREAM_ID);
        onData();

        setupHeader(3, STREAM_ID);
        onData();

        verifyBufferMapped(2);
    }

    @Test
    public void shouldAssociateBuffersWithStreams()
    {
        setupHeader(2, STREAM_ID);
        onData();

        setupHeader(2, STREAM_ID + 1);
        onData();

        verifyBufferMapped(2);
    }

    private void verifyBufferMapped(final int wantedNumberOfInvocations)
    {
        verify(mockBufferFactory, times(wantedNumberOfInvocations)).map(any(File.class), anyInt());
    }

    private void onData()
    {
        archiver.onFragment(inputBuffer, DATA_POSITION, LENGTH, mockHeader);
    }

}
