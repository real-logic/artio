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
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.generic_callback_api.FixMessageAcceptor;
import uk.co.real_logic.fix_gateway.generic_callback_api.InvalidMessageHandler;
import uk.co.real_logic.fix_gateway.util.IntHashSet;
import uk.co.real_logic.fix_gateway.util.StringFlyweight;

import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.MESSAGE_TYPE;

/**
 * Acceptor that validates messages according to a dictionary
 */
public final class GenericValidator implements FixMessageAcceptor
{

    private static final int UNKNOWN_MESSAGE_TYPE = -1;

    private final IntHashSet fieldsForMessage = new IntHashSet(1024, UNKNOWN_MESSAGE_TYPE);
    private final StringFlyweight string = new StringFlyweight(null);

    private final FixMessageAcceptor delegate;
    private final InvalidMessageHandler invalidMessageHandler;
    private final IntDictionary allFields;
    private final IntDictionary requiredFields;

    private int groupLevel = 0;

    private int messageType;
    private IntHashSet allFieldsForMessageType;

    public GenericValidator(
            final FixMessageAcceptor delegate,
            final InvalidMessageHandler invalidMessageHandler,
            final IntDictionary allFields,
            final IntDictionary requiredFields)
    {
        this.delegate = delegate;
        this.invalidMessageHandler = invalidMessageHandler;
        this.allFields = allFields;
        this.requiredFields = requiredFields;
    }

    public void onStartMessage(final long connectionId)
    {
        delegate.onStartMessage(connectionId);
        fieldsForMessage.clear();
        messageType = UNKNOWN_MESSAGE_TYPE;
        allFieldsForMessageType = null;
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
                    invalidMessageHandler.onUnknownMessage(messageType);
                    return;
                }
            }
            else if (!allFieldsForMessageType.contains(tag))
            {
                invalidMessageHandler.onUnknownField(messageType, tag);
                return;
            }
        }

        fieldsForMessage.add(tag);
        delegate.onField(tag, buffer, offset, length);
    }

    public void onGroupBegin(final int tag, final int numberOfElements)
    {
        groupLevel++;
        delegate.onGroupBegin(tag, numberOfElements);
    }

    public void onGroupEnd(final int tag)
    {
        delegate.onGroupEnd(tag);
        groupLevel--;
    }

    public void onEndMessage(final boolean passedChecksum)
    {
        final IntHashSet missingFields = requiredFields.values(messageType).difference(fieldsForMessage);
        if (missingFields == null)
        {
            delegate.onEndMessage(passedChecksum);
        }
        else
        {
            invalidMessageHandler.onMissingRequiredFields(messageType, missingFields);
            delegate.onEndMessage(false);
        }
    }

}
