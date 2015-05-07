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
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.common.protocol.HeaderFlyweight.HEADER_LENGTH;

public class ArchiverTest
{

    private static final byte DATA = (byte) 4;
    private static final int DATA_POSITION = HEADER_LENGTH + 1;
    private static final int LENGTH = HEADER_LENGTH + 10;
    private static final int STREAM_ID = 1;

    private DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
    private Header mockHeader = new Header();
    private Subscription mockSubscription = mock(Subscription.class);
    private BufferFactory mockBufferFactory = mock(BufferFactory.class);

    private ByteBuffer byteBuffer = ByteBuffer.allocate(16 * 1024);
    private UnsafeBuffer outputBuffer = new UnsafeBuffer(byteBuffer);
    private UnsafeBuffer inputBuffer = new UnsafeBuffer(new byte[16 * 1024]);

    private Archiver archiver = new Archiver(mockBufferFactory, mockSubscription);

    @Before
    public void setUp()
    {
        mockHeader.buffer(inputBuffer);
        mockHeader.initialTermId(0);
        mockHeader.offset(0);

        when(mockBufferFactory.map(anyString())).thenReturn(byteBuffer);

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
        verify(mockBufferFactory, times(wantedNumberOfInvocations)).map(anyString());
    }

    private void onData()
    {
        archiver.onData(inputBuffer, 0, LENGTH, mockHeader);
    }

}
