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
package uk.co.real_logic.fix_gateway.parser;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.generic_callback_api.FixMessageAcceptor;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.util.TestMessages.*;

public class GenericParserTest
{
    public static final int LENGTH = 16 * 1024;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[LENGTH]);
    private FixMessageAcceptor mockAcceptor = mock(FixMessageAcceptor.class);
    private GenericParser parser = new GenericParser(mockAcceptor);

    private InOrder inOrder;

    @Before
    public void setUp()
    {
        buffer.putBytes(0, EG_MESSAGE);
    }

    @Test
    public void notifiesAcceptorOfMessageStart()
    {
        when:
        parser.onMessage(buffer, 0, MSG_LEN, 1L);

        then:
        verify(mockAcceptor).onStartMessage(1L);
    }

    @Test
    public void notifiesAcceptorOfValidMessageFields()
    {
        when:
        parser.onMessage(buffer, 0, MSG_LEN, 1L);

        then:
        inOrder = inOrder(mockAcceptor);
        //8=FIX.4.2
        inOrder.verify(mockAcceptor).onField(8, buffer, 2, 7);
        //9=145
        inOrder.verify(mockAcceptor).onField(9, buffer, 12, 3);
        //35=D
        inOrder.verify(mockAcceptor).onField(35, buffer, 19, 1);
        //34=4
        inOrder.verify(mockAcceptor).onField(34, buffer, 24, 1);

        inOrder.verify(mockAcceptor, times(15)).onField(anyInt(), eq(buffer), anyInt(), anyInt());
    }

    @Test
    public void notifiesAcceptorOfValidMessageEnd()
    {
        when:
        parser.onMessage(buffer, 0, MSG_LEN, 1L);

        then:
        verify(mockAcceptor).onEndMessage(true);
    }

    @Test
    public void notifiesAcceptorOfInvalidChecksum()
    {
        given:
        buffer.putBytes(0, INVALID_CHECKSUM_MSG);

        when:
        parser.onMessage(buffer, 0, INVALID_CHECKSUM_LEN, 1L);

        then:
        verify(mockAcceptor).onEndMessage(false);
    }

    @Test
    public void notifiesAcceptorOfInvalidMessage()
    {
        given:
        buffer.putBytes(0, INVALID_MESSAGE);

        when:
        parser.onMessage(buffer, 0, INVALID_LEN, 1L);

        then:
        verify(mockAcceptor).onEndMessage(false);
    }

    // TODO: support groups
    @Ignore
    @Test
    public void notifiesAcceptorOfRepeatingGroup()
    {
        given:
        buffer.putBytes(0, EXECUTION_REPORT);

        when:
        parser.onMessage(buffer, 0, EXECUTION_REPORT_LEN, 1L);

        then:
        inOrder = Mockito.inOrder(mockAcceptor);
        inOrder.verify(mockAcceptor, times(1)).onGroupBegin(382, 1);
        inOrder.verify(mockAcceptor, times(1)).onField(437, buffer, anyInt(), anyInt());
        inOrder.verify(mockAcceptor, times(1)).onField(438, buffer, anyInt(), anyInt());
        inOrder.verify(mockAcceptor, times(1)).onGroupEnd(382);
    }


}
