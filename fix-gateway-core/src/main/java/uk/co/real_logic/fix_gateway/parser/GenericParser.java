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
import uk.co.real_logic.fix_gateway.framer.MessageHandler;
import uk.co.real_logic.fix_gateway.generic_callback_api.FixMessageAcceptor;
import uk.co.real_logic.fix_gateway.util.StringFlyweight;

import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.START_OF_HEADER;
import static uk.co.real_logic.fix_gateway.util.StringFlyweight.UNKNOWN_INDEX;

public class GenericParser implements MessageHandler
{
    private final StringFlyweight string = new StringFlyweight(null);

    private final FixMessageAcceptor acceptor;

    public GenericParser(final FixMessageAcceptor acceptor)
    {
        this.acceptor = acceptor;
    }

    public void onMessage(final DirectBuffer buffer, final int offset, final int length, final long connectionId)
    {
        string.wrap(buffer);
        final int end = offset + length;
        int position = offset;
        acceptor.onStartMessage(connectionId);

        while (position < end)
        {
            final int equalsPosition = string.scan(position, end, '=');
            if (!validatePosition(equalsPosition, acceptor))
            {
                return;
            }

            final int tag = string.getInt(position, equalsPosition);
            final int valueOffset = equalsPosition + 1;
            final int endOfField = string.scan(valueOffset, end, START_OF_HEADER);
            if (!validatePosition(endOfField, acceptor))
            {
                return;
            }

            final int valueLength = endOfField - valueOffset;

            // TODO: what should we do if onField throws an exception?
            acceptor.onField(tag, buffer, valueOffset, valueLength);

            position = endOfField + 1;
        }

        acceptor.onEndMessage(true);
    }

    private boolean validatePosition(final int position, final FixMessageAcceptor acceptor)
    {
        if (position == UNKNOWN_INDEX)
        {
            acceptor.onEndMessage(false);
            return false;
        }

        return true;
    }

}
