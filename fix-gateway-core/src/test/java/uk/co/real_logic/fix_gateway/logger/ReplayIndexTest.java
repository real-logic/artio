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
import uk.co.real_logic.fix_gateway.messages.ReplayIndexRecordDecoder;

import java.nio.ByteBuffer;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class ReplayIndexTest extends AbstractMessageTest
{

    private static final int STREAM_ID = 2;
    private ByteBuffer indexBuffer = ByteBuffer.allocate(16 * 1024);
    @SuppressWarnings("unchecked")
    private Function<String, ByteBuffer> mockBufferFactory = mock(Function.class);

    private ReplayIndex replayIndex = new ReplayIndex(mockBufferFactory);

    @Before
    public void setUp()
    {
        when(mockBufferFactory.apply(anyString())).thenReturn(indexBuffer);
    }

    @Test
    public void shouldRecordIndexEntryForFixMessage()
    {
        bufferContainsMessage(true);

        replayIndex.indexRecord(buffer, START, messageLength(), STREAM_ID);

        verifyMappedFile();

        final ReplayIndexRecordDecoder replayIndexRecord = new ReplayIndexRecordDecoder()
            .wrap(new UnsafeBuffer(indexBuffer), 8, 16, 0);

        assertEquals(STREAM_ID, replayIndexRecord.streamId());
        assertEquals(START, replayIndexRecord.position());
        assertEquals(SEQUENCE_NUMBER, replayIndexRecord.sequenceNumber());
    }

    @Test
    public void shouldOnlyMapSessionFileOnce()
    {

    }

    @Test
    public void shouldRecordIndexesForMultipleSessions()
    {
        // TODO
    }

    @Test
    public void shouldIgnoreOtherMessageTypes()
    {
        // TODO
    }

    private void verifyMappedFile()
    {
        verify(mockBufferFactory).apply(ReplayIndex.logFile(SESSION_ID));
    }

}
