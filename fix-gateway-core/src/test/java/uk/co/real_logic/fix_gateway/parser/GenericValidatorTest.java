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
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.otf_api.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.ValidationError.*;
import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.MESSAGE_TYPE;

public class GenericValidatorTest
{
    private OtfMessageAcceptor acceptor = mock(OtfMessageAcceptor.class);

    private IntDictionary requiredFields = new IntDictionary();
    private IntDictionary allFields = new IntDictionary();
    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[16 * 1024]);
    private MutableAsciiFlyweight string = new MutableAsciiFlyweight(buffer);

    private GenericValidator validator = new GenericValidator(acceptor, allFields, requiredFields);

    @Test
    public void validStartMessageDelegates()
    {
        when:
        validator.onNext();

        then:
        verify(acceptor).onNext();
    }

    @Test
    public void validEndMessageDelegates()
    {
        given:
        heartBeatsAreKnownMessages();
        messageIsAHeartBeat();
        validateMessageType();

        when:
        validator.onComplete();

        then:
        verify(acceptor).onComplete();
    }

    @Test
    public void validMessageTypeDelegates()
    {
        given:
        heartBeatsAreKnownMessages();
        messageIsAHeartBeat();

        when:
        validateMessageType();

        then:
        verifyAcceptorReceivesMessageType();
    }

    @Test
    public void invalidMessageTypeNotifiesErrorHandler()
    {
        given:
        messageIsAHeartBeat();

        when:
        validateMessageType();

        then:
        verifyAcceptorNotNotifiedOf(MESSAGE_TYPE);
        verifyUnknownMessage();
    }

    @Test
    public void validFieldDelegates()
    {
        given:
        heartbeatsHaveATestReqId();
        messageIsAHeartBeat();

        when:
        validateMessageType();
        validateTestReqId();

        then:
        verifyAcceptorReceivesMessageType();
    }

    @Test
    public void unknownFieldNotifiesErrorHandler()
    {
        given:
        heartBeatsAreKnownMessages();
        messageIsAHeartBeat();

        when:
        validateMessageType();
        validateTestReqId();

        then:
        verifyAcceptorNotNotifiedOf(112);
        verifyUnknownField();
    }

    @Test
    public void missingRequiredFieldsNotifiesErrorHandler()
    {
        given:
        heartBeatsAreKnownMessages();
        testReqIdIsARequiredHeartBeatField();
        messageIsAHeartBeat();

        when:
        validateMessageType();
        validator.onComplete();

        then:
        verifyMissingRequiredField();
    }

    private void testReqIdIsARequiredHeartBeatField()
    {
        requiredFields.put('0', 112);
    }

    private void heartbeatsHaveATestReqId()
    {
        heartBeatsAreKnownMessages();
        allFields.put('0', 112);
    }

    private void heartBeatsAreKnownMessages()
    {
        requiredFields.put('0', MESSAGE_TYPE);
        allFields.put('0', MESSAGE_TYPE);
    }

    private void messageIsAHeartBeat()
    {
        string.putAscii(0, "0");
    }

    private void validateMessageType()
    {
        validator.onField(MESSAGE_TYPE, buffer, 0, 1);
    }

    private void verifyAcceptorReceivesMessageType()
    {
        verify(acceptor).onField(MESSAGE_TYPE, buffer, 0, 1);
    }

    private void verifyAcceptorNotNotifiedOf(final int tag)
    {
        verify(acceptor, never()).onField(tag, buffer, 0, 1);
    }

    private void verifyUnknownMessage()
    {
        verify(acceptor).onError(eq(UNKNOWN_MESSAGE_TYPE), eq((int) '0'), anyInt(), any(AsciiFieldFlyweight.class));
    }

    private void verifyUnknownField()
    {
        verify(acceptor).onError(eq(UNKNOWN_FIELD), eq((int) '0'), eq(112), any(AsciiFieldFlyweight.class));
    }

    private void verifyMissingRequiredField()
    {
        verify(acceptor).onError(eq(MISSING_REQUIRED_FIELD), eq((int) '0'), eq(112), any(AsciiFieldFlyweight.class));
    }

    private void validateTestReqId()
    {
        validator.onField(112, buffer, 0, 1);
    }

}
