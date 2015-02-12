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

public class GenericValidatorTest
{
    // TODO: update to API
    /*
    private OtfMessageAcceptor acceptor = mock(OtfMessageAcceptor.class);
    private InvalidMessageHandler invalidMessageHandler = mock(InvalidMessageHandler.class);

    private IntDictionary requiredFields = new IntDictionary();
    private IntDictionary allFields = new IntDictionary();
    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[16 * 1024]);
    private MutableAsciiFlyweight string = new MutableAsciiFlyweight(buffer);

    private GenericValidator validator = new GenericValidator(acceptor, invalidMessageHandler, allFields, requiredFields);

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
        validator.onError(true);

        then:
        verify(acceptor).onError(true);
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
        validator.onError(true);

        then:
        verify(acceptor).onError(false);
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
        verify(invalidMessageHandler).onUnknownMessage('0');
    }

    private void verifyUnknownField()
    {
        verify(invalidMessageHandler).onUnknownField('0', 112);
    }

    private void verifyMissingRequiredField()
    {
        ArgumentCaptor<IntHashSet> fieldsCaptor = ArgumentCaptor.forClass(IntHashSet.class);
        verify(invalidMessageHandler).onMissingRequiredFields(eq((int) '0'), fieldsCaptor.capture());

        final IntHashSet fields = fieldsCaptor.getValue();
        assertTrue(fields.contains(112));
    }

    private void validateTestReqId()
    {
        validator.onField(112, buffer, 0, 1);
    }
    */

}
