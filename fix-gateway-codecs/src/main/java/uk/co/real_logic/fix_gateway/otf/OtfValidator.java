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

import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.fix_gateway.ValidationError;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;

import static uk.co.real_logic.fix_gateway.ValidationError.*;
import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.MESSAGE_TYPE;

/**
 * Acceptor that validates messages according to a dictionary
 */
public final class OtfValidator implements OtfMessageAcceptor
{
    private static final int UNKNOWN = -1;

    private final IntHashSet fieldsForMessage = new IntHashSet(1024, UNKNOWN);
    private final AsciiFieldFlyweight stringField = new AsciiFieldFlyweight();

    private final OtfMessageAcceptor delegate;
    private final IntDictionary allFields;
    private final IntDictionary requiredFields;

    private int groupLevel = 0;

    private int messageType;
    private IntHashSet allFieldsForMessageType;

    public OtfValidator(
        final OtfMessageAcceptor delegate,
        final IntDictionary allFields,
        final IntDictionary requiredFields)
    {
        this.delegate = delegate;
        this.allFields = allFields;
        this.requiredFields = requiredFields;
    }

    public MessageControl onNext()
    {
        delegate.onNext();
        fieldsForMessage.clear();
        messageType = UNKNOWN;
        allFieldsForMessageType = null;
        return MessageControl.CONTINUE;
    }

    @Override
    public MessageControl onComplete()
    {
        final IntHashSet missingFields = requiredFields.values(messageType).difference(fieldsForMessage);
        if (missingFields == null)
        {
            return delegate.onComplete();
        }
        else
        {
            for (final int value : missingFields)
            {
                delegate.onError(MISSING_REQUIRED_FIELD, messageType, value, stringField);
            }
        }
        return MessageControl.CONTINUE;
    }

    public MessageControl onField(final int tag, final AsciiBuffer buffer, final int offset, final int length)
    {
        if (groupLevel == 0)
        {
            if (tag == MESSAGE_TYPE)
            {
                messageType = buffer.getMessageType(offset, length);
                allFieldsForMessageType = allFields.values(messageType);
                if (allFieldsForMessageType == null)
                {
                    delegate.onError(UNKNOWN_MESSAGE_TYPE, messageType, UNKNOWN, stringField);
                    return MessageControl.STOP;
                }
            }
            else if (!allFieldsForMessageType.contains(tag))
            {
                delegate.onError(UNKNOWN_FIELD, messageType, tag, stringField);
                return MessageControl.STOP;
            }
        }

        fieldsForMessage.add(tag);
        return delegate.onField(tag, buffer, offset, length);
    }

    @Override
    public MessageControl onGroupHeader(int tag, int numInGroup)
    {
        groupLevel++;
        return delegate.onGroupHeader(tag, numInGroup);
    }

    @Override
    public MessageControl onGroupBegin(int tag, int numInGroup, int index)
    {
        return delegate.onGroupBegin(tag, numInGroup, index);
    }

    @Override
    public MessageControl onGroupEnd(int tag, int numInGroup, int index)
    {
        final MessageControl control = delegate.onGroupEnd(tag, numInGroup, index);
        if (numInGroup == index + 1)
        {
            groupLevel--;
        }
        return control;
    }

    @Override
    public boolean onError(ValidationError error, int messageType, int tagNumber, AsciiFieldFlyweight value)
    {
        return delegate.onError(error, messageType, tagNumber, value);
    }
}
