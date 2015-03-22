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
package uk.co.real_logic.fix_gateway.otf;

import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.ValidationError.INVALID_CHECKSUM;
import static uk.co.real_logic.fix_gateway.ValidationError.PARSE_ERROR;
import static uk.co.real_logic.fix_gateway.util.TestMessages.*;


@RunWith(Theories.class)
public class OtfParserTest
{
    @DataPoint
    public static int NO_OFFSET = 0;

    @DataPoint
    public static int OFFSET = 1;

    private static final int MESSAGE_TYPE = 'D';

    public static final int LENGTH = 16 * 1024;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[LENGTH]);
    private OtfMessageAcceptor mockAcceptor = mock(OtfMessageAcceptor.class);
    private IntDictionary groupToField = new IntDictionary();
    private OtfParser parser = new OtfParser(mockAcceptor, groupToField);

    private InOrder inOrder = inOrder(mockAcceptor);

    @Theory
    public void notifiesAcceptorOfMessageStart(final int offset)
    {
        given:
        putMessage(offset);

        when:
        parser.onMessage(buffer, offset, MSG_LEN, 1L, MESSAGE_TYPE);

        then:
        verify(mockAcceptor).onNext();
    }

    @Theory
    public void notifiesAcceptorOfValidMessageFields(final int offset)
    {
        given:
        putMessage(offset);

        when:
        parser.onMessage(buffer, offset, MSG_LEN, 1L, MESSAGE_TYPE);

        then:
        //8=FIX.4.2
        inOrder.verify(mockAcceptor).onField(8, buffer, offset + 2, 7);
        //9=145
        inOrder.verify(mockAcceptor).onField(9, buffer, offset + 12, 3);
        //35=D
        inOrder.verify(mockAcceptor).onField(35, buffer, offset + 19, 1);
        //34=4
        inOrder.verify(mockAcceptor).onField(34, buffer, offset + 24, 1);

        inOrder.verify(mockAcceptor, times(15)).onField(anyInt(), eq(buffer), anyInt(), anyInt());
    }

    @Theory
    public void notifiesAcceptorOfValidMessageEnd(final int offset)
    {
        given:
        putMessage(offset);

        when:
        parser.onMessage(buffer, offset, MSG_LEN, 1L, MESSAGE_TYPE);

        then:
        verify(mockAcceptor).onComplete();
    }

    @Theory
    public void notifiesAcceptorOfInvalidChecksum(final int offset)
    {
        given:
        buffer.putBytes(offset, INVALID_CHECKSUM_MSG);

        when:
        parser.onMessage(buffer, offset, INVALID_CHECKSUM_LEN, 1L, MESSAGE_TYPE);

        then:
        verify(mockAcceptor).onError(eq(INVALID_CHECKSUM), eq((int) 'D'), eq(10), any(AsciiFieldFlyweight.class));
    }

    @Theory
    public void notifiesAcceptorOfInvalidMessage(final int offset)
    {
        given:
        buffer.putBytes(offset, INVALID_MESSAGE);

        when:
        parser.onMessage(buffer, offset, INVALID_LEN, 1L, MESSAGE_TYPE);

        then:
        verify(mockAcceptor).onError(eq(PARSE_ERROR), eq((int) 'D'), eq(11), any(AsciiFieldFlyweight.class));
    }

    @Theory
    public void notifiesAcceptorOfRepeatingGroup(final int offset)
    {
        given:
        understandsContraBrokersGroup();

        buffer.putBytes(offset, EXECUTION_REPORT);

        when:
        parser.onMessage(buffer, offset, EXECUTION_REPORT.length, 1L, MESSAGE_TYPE);

        then:
        verifyGroupHeader(382, 1);
        verifyGroupBegin(382, 1, 0);
        verifyInOrderField(375);
        verifyInOrderField(337);
        verifyInOrderField(437);
        verifyInOrderField(438);
        verifyGroupEnd(382, 1, 0);
    }

    @Theory
    public void notifiesAcceptorOfOnlyHeaderForEmptyGroup(final int offset)
    {
        given:
        understandsContraBrokersGroup();
        buffer.putBytes(offset, ZERO_REPEATING_GROUP);

        when:
        parser.onMessage(buffer, offset, ZERO_REPEATING_GROUP.length, 1L, MESSAGE_TYPE);

        then:
        verifyGroupHeader(382, 0);
        inOrder.verify(mockAcceptor, never()).onGroupBegin(anyInt(), anyInt(), anyInt());
        inOrder.verify(mockAcceptor, never()).onGroupEnd(anyInt(), anyInt(), anyInt());
    }

    @Theory
    public void notifiesAcceptorOfMultiElementRepeatingGroup(final int offset)
    {
        given:
        understandsNoOrdersGroup();

        buffer.putBytes(offset, REPEATING_GROUP);

        when:
        parser.onMessage(buffer, offset, REPEATING_GROUP.length, 1L, MESSAGE_TYPE);

        then:
        verifyGroupHeader(NO_ORDERS, 2);
        verifyNoOrdersGroup(0);
        verifyNoOrdersGroup(1);
    }

    @Theory
    public void notifiesAcceptorOfNestedRepeatingGroup(final int offset)
    {
        given:
        understandsNoOrdersGroup();
        understandsNoAllocsGroup();

        buffer.putBytes(offset, NESTED_REPEATING_GROUP);

        when:
        parser.onMessage(buffer, offset, NESTED_REPEATING_GROUP.length, 1L, MESSAGE_TYPE);

        then:
        verifyGroupHeader(NO_ORDERS, 2);
        verifyNestedNoAllocsGroup(0);
        verifyNoOrdersGroup(1);
    }

    private void verifyGroupHeader(final int groupNumber, final int numberOfElements)
    {
        inOrder.verify(mockAcceptor, times(1)).onGroupHeader(groupNumber, numberOfElements);
    }

    private void verifyGroupBegin(final int groupNumber, final int numberOfElements, final int index)
    {
        inOrder.verify(mockAcceptor, times(1)).onGroupBegin(groupNumber, numberOfElements, index);
    }

    private void verifyGroupEnd(final int groupNumber, final int numberOfElements, final int index)
    {
        inOrder.verify(mockAcceptor, times(1)).onGroupEnd(groupNumber, numberOfElements, index);
    }

    private void verifyNoOrdersGroup(final int index)
    {
        verifyNoOrdersFields(index);
        verifyGroupEnd(NO_ORDERS, 2, index);
    }

    private void verifyNestedNoAllocsGroup(final int index)
    {
        verifyNoOrdersFields(index);

        verifyGroupHeader(NO_ALLOCS, 2);
        verifyNoAllocsGroup(0);
        verifyNoAllocsGroup(1);

        verifyGroupEnd(NO_ORDERS, 2, index);
    }

    private void verifyNoOrdersFields(final int index)
    {
        verifyGroupBegin(NO_ORDERS, 2, index);
        verifyInOrderField(11);
        verifyInOrderField(67);
        verifyInOrderField(55);
        verifyInOrderField(54);
        verifyInOrderField(38);
        verifyInOrderField(40);
    }

    private void verifyNoAllocsGroup(final int index)
    {
        verifyGroupBegin(NO_ALLOCS, 2, index);
        verifyInOrderField(79);
        verifyInOrderField(467);
        verifyInOrderField(366);
        verifyGroupEnd(NO_ALLOCS, 2, index);
    }

    private void putMessage(final int offset)
    {
        buffer.putBytes(offset, EG_MESSAGE);
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
