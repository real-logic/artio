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

import org.junit.Test;

import java.nio.MappedByteBuffer;
import java.util.function.Function;

import static org.mockito.Mockito.mock;

public class ReplayIndexTest extends AbstractMessageTest
{

    @SuppressWarnings("unchecked")
    private Function<String, MappedByteBuffer> mockBufferFactory = mock(Function.class);
    private ReplayIndex replayIndex = new ReplayIndex(mockBufferFactory);

    @Test
    public void shouldRecordIndexEntryForFixMessage()
    {
        bufferContainsMessage(true);

        replayIndex.indexRecord(buffer, START, messageLength());

        // TODO
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

}
