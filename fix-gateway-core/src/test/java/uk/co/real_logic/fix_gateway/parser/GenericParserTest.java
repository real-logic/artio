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
import org.junit.Test;
import org.mockito.InOrder;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.otf_api.OtfMessageAcceptor;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.ValidationError.INVALID_CHECKSUM;
import static uk.co.real_logic.fix_gateway.ValidationError.PARSE_ERROR;
import static uk.co.real_logic.fix_gateway.util.TestMessages.*;


public class GenericParserTest
{
    // TODO: Update to API

    public static final int LENGTH = 16 * 1024;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[LENGTH]);
    private OtfMessageAcceptor mockAcceptor = mock(OtfMessageAcceptor.class);
    private IntDictionary groupToField = new IntDictionary();
    private GenericParser parser = new GenericParser(mockAcceptor, groupToField);

    private InOrder inOrder = inOrder(mockAcceptor);

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
        verify(mockAcceptor).onNext();
    }

    @Test
    public void notifiesAcceptorOfValidMessageFields()
    {
        when:
        parser.onMessage(buffer, 0, MSG_LEN, 1L);

        then:
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
        verify(mockAcceptor).onComplete();
    }

    @Test
    public void notifiesAcceptorOfInvalidChecksum()
    {
        given:
        buffer.putBytes(0, INVALID_CHECKSUM_MSG);

        when:
        parser.onMessage(buffer, 0, INVALID_CHECKSUM_LEN, 1L);

        then:
        verify(mockAcceptor).onError(eq(INVALID_CHECKSUM), eq((int) 'D'), eq(10), any(AsciiFieldFlyweight.class));
    }

    @Test
    public void notifiesAcceptorOfInvalidMessage()
    {
        given:
        buffer.putBytes(0, INVALID_MESSAGE);

        when:
        parser.onMessage(buffer, 0, INVALID_LEN, 1L);

        then:
        verify(mockAcceptor).onError(eq(PARSE_ERROR), eq((int) 'D'), eq(11), any(AsciiFieldFlyweight.class));
    }

    // TODO: change group parsing code to reflect updated API
    @Test
    public void notifiesAcceptorOfRepeatingGroup()
    {
        given:
        understandsContraBrokersGroup();

        buffer.putBytes(0, EXECUTION_REPORT);

        when:
        parser.onMessage(buffer, 0, EXECUTION_REPORT.length, 1L);

        then:
        verifyGroupBegin(382, 1);
        verifyInOrderField(375);
        verifyInOrderField(337);
        verifyInOrderField(437);
        verifyInOrderField(438);
        verifyGroupEnd(382);
    }

    // TODO: change normalisation to get header callbacks for each group
    @Test
    public void normalisesAwayEmptyRepeatingGroup()
    {
        given:
        understandsContraBrokersGroup();
        buffer.putBytes(0, ZERO_REPEATING_GROUP);

        when:
        parser.onMessage(buffer, 0, ZERO_REPEATING_GROUP.length, 1L);

        then:
        inOrder.verify(mockAcceptor, never()).onGroupBegin(eq(382), anyInt(), eq(0));
        inOrder.verify(mockAcceptor, never()).onGroupEnd(382, 0, 0);
    }

    @Test
    public void notifiesAcceptorOfMultiElementRepeatingGroup()
    {
        given:
        understandsNoOrdersGroup();

        buffer.putBytes(0, REPEATING_GROUP);

        when:
        parser.onMessage(buffer, 0, REPEATING_GROUP.length, 1L);

        then:
        verifyGroupBegin(73, 2);
        verifyNoOrdersFields();
        verifyNoOrdersFields();
        verifyGroupEnd(73);
    }

    @Test
    public void notifiesAcceptorOfNestedRepeatingGroup()
    {
        given:
        understandsNoOrdersGroup();
        understandsNoAllocsGroup();

        buffer.putBytes(0, NESTED_REPEATING_GROUP);

        when:
        parser.onMessage(buffer, 0, NESTED_REPEATING_GROUP.length, 1L);

        then:
        verifyGroupBegin(73, 2);

        verifyNoOrdersFields();

        verifyGroupBegin(78, 2);
        verifyNoAllocsFields();
        verifyGroupEnd(78);

        verifyNoOrdersFields();

        verifyGroupEnd(73);
    }

    private void verifyGroupBegin(final int groupNumber, final int numberOfElements)
    {
        inOrder.verify(mockAcceptor, times(1)).onGroupBegin(groupNumber, numberOfElements, 0);
    }

    private void verifyGroupEnd(final int groupNumber)
    {
        inOrder.verify(mockAcceptor, times(1)).onGroupEnd(groupNumber, 0, 0);
    }

    private void verifyNoOrdersFields()
    {
        verifyInOrderField(11);
        verifyInOrderField(67);
        verifyInOrderField(55);
        verifyInOrderField(54);
        verifyInOrderField(38);
        verifyInOrderField(40);
    }

    private void verifyNoAllocsFields()
    {
        verifyInOrderField(79);
        verifyInOrderField(467);
        verifyInOrderField(366);
    }

    private void understandsContraBrokersGroup()
    {
        groupToField.putAll(382, 337, 375, 437, 438);
    }

    private void understandsNoOrdersGroup()
    {
        groupToField.putAll(73, 11, 67, 55, 54, 38, 40, 78);
    }

    private void understandsNoAllocsGroup()
    {
        groupToField.putAll(78, 79, 467, 366);
    }

    private void verifyInOrderField(final int tag)
    {
        inOrder.verify(mockAcceptor, times(1)).onField(eq(tag), eq(buffer), anyInt(), anyInt());
    }


}
