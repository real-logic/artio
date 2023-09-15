/*
 * Copyright 2015-2023 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.otf;

import org.junit.Test;
import uk.co.real_logic.artio.dictionary.LongDictionary;
import uk.co.real_logic.artio.fields.AsciiFieldFlyweight;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.ValidationError.*;
import static uk.co.real_logic.artio.dictionary.SessionConstants.MESSAGE_TYPE;

public class OtfValidatorTest
{
    private final OtfMessageAcceptor acceptor = mock(OtfMessageAcceptor.class);

    private final LongDictionary requiredFields = new LongDictionary();
    private final LongDictionary allFields = new LongDictionary();
    private final MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[16 * 1024]);

    private final OtfValidator validator = new OtfValidator(acceptor, allFields, requiredFields);

    @Test
    public void validStartMessageDelegates()
    {
        validator.onNext();

        verify(acceptor).onNext();
    }

    @Test
    public void validEndMessageDelegates()
    {
        heartBeatsAreKnownMessages();
        messageIsAHeartBeat();
        validateMessageType();

        validator.onComplete();

        verify(acceptor).onComplete();
    }

    @Test
    public void validMessageTypeDelegates()
    {
        heartBeatsAreKnownMessages();
        messageIsAHeartBeat();

        validateMessageType();

        verifyAcceptorReceivesMessageType();
    }

    @Test
    public void invalidMessageTypeNotifiesErrorHandler()
    {
        messageIsAHeartBeat();

        validateMessageType();

        verifyAcceptorNotNotifiedOf(MESSAGE_TYPE);
        verifyUnknownMessage();
    }

    @Test
    public void validFieldDelegates()
    {
        heartbeatsHaveATestReqId();
        messageIsAHeartBeat();

        validateMessageType();
        validateTestReqId();

        verifyAcceptorReceivesMessageType();
    }

    @Test
    public void unknownFieldNotifiesErrorHandler()
    {
        heartBeatsAreKnownMessages();
        messageIsAHeartBeat();

        validateMessageType();
        validateTestReqId();

        verifyAcceptorNotNotifiedOf(112);
        verifyUnknownField();
    }

    @Test
    public void missingRequiredFieldsNotifiesErrorHandler()
    {
        heartBeatsAreKnownMessages();
        testReqIdIsARequiredHeartBeatField();
        messageIsAHeartBeat();

        validateMessageType();
        validator.onComplete();

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
        buffer.putAscii(0, "0");
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
        verify(acceptor).onError(eq(UNKNOWN_MESSAGE_TYPE), eq((long)'0'), anyInt(), any(AsciiFieldFlyweight.class));
    }

    private void verifyUnknownField()
    {
        verify(acceptor).onError(eq(UNKNOWN_FIELD), eq((long)'0'), eq(112), any(AsciiFieldFlyweight.class));
    }

    private void verifyMissingRequiredField()
    {
        verify(acceptor).onError(eq(MISSING_REQUIRED_FIELD), eq((long)'0'), eq(112), any(AsciiFieldFlyweight.class));
    }

    private void validateTestReqId()
    {
        validator.onField(112, buffer, 0, 1);
    }
}
