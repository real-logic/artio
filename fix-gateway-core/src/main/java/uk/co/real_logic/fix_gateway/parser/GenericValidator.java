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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.ValidationError;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.otf_api.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.reactive_api.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;
import uk.co.real_logic.fix_gateway.util.IntHashSet;

import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.MESSAGE_TYPE;

/**
 * Acceptor that validates messages according to a dictionary
 */
public final class GenericValidator implements OtfMessageAcceptor
{

    private static final int UNKNOWN_MESSAGE_TYPE = -1;

    private final IntHashSet fieldsForMessage = new IntHashSet(1024, UNKNOWN_MESSAGE_TYPE);
    private final AsciiFlyweight string = new AsciiFlyweight(null);

    private final OtfMessageAcceptor delegate;
    private final IntDictionary allFields;
    private final IntDictionary requiredFields;

    private int groupLevel = 0;

    private int messageType;
    private IntHashSet allFieldsForMessageType;

    public GenericValidator(
            final OtfMessageAcceptor delegate,
            final IntDictionary allFields,
            final IntDictionary requiredFields)
    {
        this.delegate = delegate;
        this.allFields = allFields;
        this.requiredFields = requiredFields;
    }

    public void onNext()
    {
        delegate.onNext();
        fieldsForMessage.clear();
        messageType = UNKNOWN_MESSAGE_TYPE;
        allFieldsForMessageType = null;
    }

    @Override
    public void onComplete()
    {
        // TODO: update to API
        final IntHashSet missingFields = requiredFields.values(messageType).difference(fieldsForMessage);
        if (missingFields == null)
        {
            //delegate.onError(passedChecksum);
        }
        else
        {
            //invalidMessageHandler.onMissingRequiredFields(messageType, missingFields);
            //delegate.onError(false);
        }
    }

    public void onField(final int tag, final DirectBuffer buffer, final int offset, final int length)
    {
        if (groupLevel == 0)
        {
            if (tag == MESSAGE_TYPE)
            {
                string.wrap(buffer);
                messageType = string.getMessageType(offset, length);
                allFieldsForMessageType = allFields.values(messageType);
                if (allFieldsForMessageType == null)
                {
                    // TODO: invalidMessageHandler.onUnknownMessage(messageType);
                    return;
                }
            }
            else if (!allFieldsForMessageType.contains(tag))
            {
                // TODO: invalidMessageHandler.onUnknownField(messageType, tag);
                return;
            }
        }

        fieldsForMessage.add(tag);
        delegate.onField(tag, buffer, offset, length);
    }

    @Override
    public void onGroupHeader(int tag, int numberOfElements)
    {
        groupLevel++;
    }

    @Override
    public void onGroupBegin(int tag, int numberOfElements, int index)
    {

        delegate.onGroupBegin(tag, numberOfElements, index);
    }

    @Override
    public void onGroupEnd(int tag, int numberOfElements, int index)
    {
        delegate.onGroupEnd(tag, numberOfElements, index);
        if (numberOfElements == index + 1)
        {
            groupLevel--;
        }
    }

    @Override
    public boolean onError(ValidationError error, int messageType, int tagNumber, AsciiFieldFlyweight value)
    {
        return false;
    }
}
