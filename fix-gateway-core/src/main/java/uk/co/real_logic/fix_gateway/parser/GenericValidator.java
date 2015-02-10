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
import uk.co.real_logic.fix_gateway.dictionary.ValidationDictionary;
import uk.co.real_logic.fix_gateway.generic_callback_api.FixMessageAcceptor;
import uk.co.real_logic.fix_gateway.generic_callback_api.InvalidMessageHandler;
import uk.co.real_logic.fix_gateway.util.StringFlyweight;

import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.MESSAGE_TYPE;

/**
 * Acceptor that validates messages according to a dictionary
 */
public final class GenericValidator implements FixMessageAcceptor
{

    private final FixMessageAcceptor delegate;
    private final InvalidMessageHandler invalidMessageHandler;
    private final ValidationDictionary allFields;

    private final StringFlyweight string = new StringFlyweight(null);

    public GenericValidator(
            final FixMessageAcceptor delegate,
            final InvalidMessageHandler invalidMessageHandler,
            final ValidationDictionary allFields)
    {
        this.delegate = delegate;
        this.invalidMessageHandler = invalidMessageHandler;
        this.allFields = allFields;
    }

    public void onStartMessage(final long connectionId)
    {
        delegate.onStartMessage(connectionId);
    }

    public void onField(final int tag, final DirectBuffer buffer, final int offset, final int length)
    {
        if (tag == MESSAGE_TYPE)
        {
            string.wrap(buffer);
            final int messageType = string.getMessageType(offset, length);
            if (allFields.fields(messageType) == null)
            {
                invalidMessageHandler.onInvalidMessage(messageType);
                return;
            }
        }

        delegate.onField(tag, buffer, offset, length);
    }

    public void onGroupBegin(final int tag, final int numberOfElements)
    {

    }

    public void onGroupEnd(final int tag)
    {

    }

    public void onEndMessage(final boolean passedChecksum)
    {
        delegate.onEndMessage(passedChecksum);
    }

}
