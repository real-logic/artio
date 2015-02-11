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
import uk.co.real_logic.fix_gateway.framer.MessageHandler;
import uk.co.real_logic.fix_gateway.generic_callback_api.FixMessageAcceptor;
import uk.co.real_logic.fix_gateway.util.IntHashSet;
import uk.co.real_logic.fix_gateway.util.StringFlyweight;

import java.util.ArrayDeque;
import java.util.Deque;

import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.CHECKSUM;
import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.START_OF_HEADER;
import static uk.co.real_logic.fix_gateway.util.StringFlyweight.UNKNOWN_INDEX;

// TODO: what should we do if the callbacks throw an exception?
public final class GenericParser implements MessageHandler
{
    private static final int NO_CHECKSUM = 0;
    private static final int NO_GROUP = -1;

    private final StringFlyweight string = new StringFlyweight(null);
    private final Deque<IntHashSet> outerGroupFields = new ArrayDeque<>();
    private final Deque<Integer> outerGroupNumber = new ArrayDeque<>();

    private final FixMessageAcceptor acceptor;
    private final IntDictionary groupToField;

    private IntHashSet currentGroupFields = null;
    private int currentGroupNumber = NO_GROUP;

    public GenericParser(final FixMessageAcceptor acceptor, final IntDictionary groupToField)
    {
        this.acceptor = acceptor;
        this.groupToField = groupToField;
    }

    public void onMessage(final DirectBuffer buffer, final int offset, final int length, final long connectionId)
    {
        string.wrap(buffer);
        acceptor.onStartMessage(connectionId);

        final int end = offset + length;
        int position = offset;

        int checksum = NO_CHECKSUM;
        int checksumOffset = 0;
        try
        {
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

                final IntHashSet newGroupFields = groupToField.values(tag);
                if (newGroupFields == null)
                {
                    checkGroupEnd(tag);

                    acceptor.onField(tag, buffer, valueOffset, valueLength);

                    if (tag == CHECKSUM)
                    {
                        checksum = string.getInt(valueOffset, endOfField);
                        checksumOffset = equalsPosition - 2;
                    }
                }
                else
                {
                    groupBegin(tag, valueOffset, endOfField, newGroupFields);
                }

                position = endOfField + 1;
            }

            // While due to the possibility of nested groups all ending at the end of the message
            while (insideAGroup())
            {
                endGroup();
            }

            acceptor.onEndMessage(validateChecksum(buffer, offset, checksumOffset, checksum));
        }
        catch (IllegalArgumentException e)
        {
            //e.printStackTrace();
            // Error parsing the message
            acceptor.onEndMessage(false);
        }
    }

    private boolean insideAGroup()
    {
        return this.currentGroupNumber != NO_GROUP;
    }

    private void checkGroupEnd(final int tag)
    {
        if (insideAGroup() && !fieldIsInCurrentGroup(tag))
        {
            endGroup();
        }
    }

    private void endGroup()
    {
        acceptor.onGroupEnd(currentGroupNumber);
        currentGroupFields = outerGroupFields.poll();
        currentGroupNumber = currentGroupFields == null ? NO_GROUP : outerGroupNumber.poll();
    }

    private void groupBegin(final int tag, final int valueOffset, final int endOfField, final IntHashSet newGroupFields)
    {
        final int numberOfElements = string.getInt(valueOffset, endOfField);
        // Normalise away empty repeating groups, since its valid to leave them out anyway
        if (numberOfElements > 0)
        {
            if (insideAGroup())
            {
                outerGroupFields.push(this.currentGroupFields);
                outerGroupNumber.push(Integer.valueOf(this.currentGroupNumber));
            }

            this.currentGroupFields = newGroupFields;
            this.currentGroupNumber = tag;
            acceptor.onGroupBegin(tag, numberOfElements);
        }
    }

    private boolean fieldIsInCurrentGroup(final int tag)
    {
        return this.currentGroupFields != null && this.currentGroupFields.contains(tag);
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

    private boolean validateChecksum(final DirectBuffer buffer, final int offset, final int length, final int checksum)
    {
        if (checksum == NO_CHECKSUM)
        {
            return false;
        }

        final int end = offset + length;

        long total = 0L;
        for (int index = offset; index < end; index++)
        {
            total += (int) buffer.getByte(index);
        }

        return (total % 256) == checksum;
    }

}
