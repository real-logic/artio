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
package uk.co.real_logic.parser;

import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.generic_callback_api.FixMessageAcceptor;
import uk.co.real_logic.fix_gateway.parser.GenericParser;

import static org.mockito.Mockito.mock;

public class GenericParserTest
{
    public static final int LENGTH = 16 * 1024;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[LENGTH]);
    private FixMessageAcceptor mockAcceptor = mock(FixMessageAcceptor.class);
    private GenericParser parser = new GenericParser(mockAcceptor);

    @Test
    public void parserNotifiesAcceptorOfMessageStart()
    {
        when:
        parser.onMessage(buffer, 0, LENGTH, 1L);

        then:
        mockAcceptor.onStartMessage();
    }

}
