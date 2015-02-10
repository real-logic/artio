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

import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.dictionary.ValidationDictionary;
import uk.co.real_logic.fix_gateway.generic_callback_api.FixMessageAcceptor;
import uk.co.real_logic.fix_gateway.generic_callback_api.InvalidMessageHandler;
import uk.co.real_logic.fix_gateway.util.MutableStringFlyweight;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.MESSAGE_TYPE;

public class GenericValidatorTest
{
    private FixMessageAcceptor acceptor = mock(FixMessageAcceptor.class);
    private InvalidMessageHandler invalidMessageHandler = mock(InvalidMessageHandler.class);

    private ValidationDictionary requiredFields = new ValidationDictionary();
    private ValidationDictionary allFields = new ValidationDictionary();
    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[16 * 1024]);
    private MutableStringFlyweight string = new MutableStringFlyweight(buffer);

    private GenericValidator validator = new GenericValidator(acceptor, invalidMessageHandler, allFields);

    @Test
    public void validStartMessageDelegates()
    {
        when:
        validator.onStartMessage(1L);

        then:
        verify(acceptor).onStartMessage(1L);
    }

    @Test
    public void validEndMessageDelegates()
    {
        when:
        validator.onEndMessage(true);

        then:
        verify(acceptor).onEndMessage(true);
    }

    @Test
    public void validMessageTypeDelegates()
    {
        given:
        allFields.put('0', MESSAGE_TYPE);
        string.putAscii(0, "0");

        when:
        validator.onField(MESSAGE_TYPE, buffer, 0, 1);

        then:
        verify(acceptor).onField(MESSAGE_TYPE, buffer, 0, 1);
    }

    @Test
    public void invalidMessageTypeNotifiesErrorHandler()
    {
        given:
        string.putAscii(0, "0");

        when:
        validator.onField(MESSAGE_TYPE, buffer, 0, 1);

        then:
        verify(acceptor, never()).onField(MESSAGE_TYPE, buffer, 0, 1);
        verify(invalidMessageHandler).onInvalidMessage('0');
    }

    @Test
    public void missingRequiredFieldsNotifiesErrorHandler()
    {

    }

    @Test
    public void unknownFieldNotifiesErrorHandler()
    {

    }

}
