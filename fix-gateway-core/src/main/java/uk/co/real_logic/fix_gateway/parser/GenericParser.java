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
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.framer.MessageHandler;
import uk.co.real_logic.fix_gateway.otf_api.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;
import uk.co.real_logic.fix_gateway.util.IntHashSet;

import static uk.co.real_logic.fix_gateway.ValidationError.PARSE_ERROR;
import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.*;
import static uk.co.real_logic.fix_gateway.util.AsciiFlyweight.UNKNOWN_INDEX;

// TODO: what should we do if the callbacks throw an exception?
public final class GenericParser implements MessageHandler
{
    public static final int MAX_NUMBER_OF_GROUPS = 1024;

    private static final int NO_CHECKSUM = 0;
    private static final int UNKNOWN = -1;

    private final AsciiFlyweight string = new AsciiFlyweight(null);
    private final AsciiFieldFlyweight stringField = new AsciiFieldFlyweight();
    /*private final Deque<IntHashSet> outerGroupFields = new ArrayDeque<>();
    private final Deque<Integer> outerGroupNumber = new ArrayDeque<>();*/

    private final OtfMessageAcceptor acceptor;
    private final IntDictionary groupToField;

    private int currentGroupTag = UNKNOWN;
    private IntHashSet currentGroupFields = null;
    private int currentGroupFirstField;
    private int currentGroupNumberOfElements;
    private int currentGroupIndex;

    public GenericParser(final OtfMessageAcceptor acceptor, final IntDictionary groupToField)
    {
        this.acceptor = acceptor;
        this.groupToField = groupToField;
    }

    public void onMessage(final DirectBuffer buffer, final int offset, final int length, final long connectionId)
    {
        string.wrap(buffer);
        acceptor.onNext();

        final int end = offset + length;
        int position = offset;

        int checksum = NO_CHECKSUM;
        int checksumOffset = 0;
        int messageType = UNKNOWN;
        int tag = UNKNOWN;
        try
        {
            while (position < end)
            {
                final int equalsPosition = string.scan(position, end, '=');
                if (!validatePosition(equalsPosition, acceptor, messageType, tag))
                {
                    return;
                }

                tag = string.getInt(position, equalsPosition);
                final int valueOffset = equalsPosition + 1;
                final int endOfField = string.scan(valueOffset, end, START_OF_HEADER);
                if (!validatePosition(endOfField, acceptor, messageType, tag))
                {
                    return;
                }

                final int valueLength = endOfField - valueOffset;

                final IntHashSet newGroupFields = groupToField.values(tag);
                if (newGroupFields == null)
                {
                    checkGroup(tag);

                    acceptor.onField(tag, buffer, valueOffset, valueLength);

                    if (tag == CHECKSUM)
                    {
                        checksum = string.getInt(valueOffset, endOfField);
                        checksumOffset = equalsPosition - 2;
                    }
                    else if(tag == MESSAGE_TYPE)
                    {
                        messageType = string.getMessageType(valueOffset, valueLength);
                    }
                }
                else
                {
                    groupHeader(tag, valueOffset, endOfField, newGroupFields);
                }

                position = endOfField + 1;
            }

            // While due to the possibility of nested groups all ending at the end of the message
            while (insideAGroup())
            {
                endRepeatingGroupBlock();
            }

            if (validateChecksum(buffer, offset, checksumOffset, checksum))
            {
                acceptor.onComplete();
            }
            else
            {
                acceptor.onError(ValidationError.INVALID_CHECKSUM, messageType, CHECKSUM, stringField);
            }
        }
        catch (IllegalArgumentException e)
        {
            // Error parsing the message
            // e.printStackTrace();
            acceptor.onError(PARSE_ERROR, messageType, tag, stringField);
        }
    }

    private void checkGroup(final int tag)
    {
        if (insideAGroup())
        {
            // Non-group field means end of group
            if (!currentGroupFields.contains(tag))
            {
                endRepeatingGroupBlock();
            }
            else
            {
                // First field first iteration
                if (currentGroupFirstField == UNKNOWN)
                {
                    currentGroupFirstField = tag;
                }
                // We've seen the first field again - its a new group iteration
                else if(tag == currentGroupFirstField)
                {
                    onEndGroup();
                    currentGroupIndex++;
                    onGroupBegin();
                }
            }
        }
    }

    private boolean insideAGroup()
    {
        return this.currentGroupTag != UNKNOWN;
    }

    private void endRepeatingGroupBlock()
    {
        onEndGroup();
        currentGroupTag = UNKNOWN;
    }

    private void onEndGroup()
    {
        acceptor.onGroupEnd(currentGroupTag, currentGroupNumberOfElements, currentGroupIndex);

        // TODO:
        /*currentGroupFields = outerGroupFields.poll();
        currentGroupNumber = currentGroupFields == null ? UNKNOWN : outerGroupNumber.poll();*/

    }

    private void groupHeader(final int tag, final int valueOffset, final int endOfField, final IntHashSet newGroupFields)
    {
        final int numberOfElements = string.getInt(valueOffset, endOfField);
        // Normalise away empty repeating groups, since its valid to leave them out anyway

            // TODO:
            /*if (insideAGroup())
            {
                outerGroupFields.push(this.currentGroupFields);
                outerGroupNumber.push(Integer.valueOf(this.currentGroupTag));
            }*/
        acceptor.onGroupHeader(tag, numberOfElements);

        if (numberOfElements > 0)
        {
            currentGroupTag = tag;
            currentGroupFields = newGroupFields;
            currentGroupFirstField = UNKNOWN;
            currentGroupNumberOfElements = numberOfElements;
            currentGroupIndex = 0;

            onGroupBegin();
        }
    }

    private void onGroupBegin()
    {
        acceptor.onGroupBegin(currentGroupTag, currentGroupNumberOfElements, currentGroupIndex);
    }

    private boolean validatePosition(final int position, final OtfMessageAcceptor acceptor, final int messageType, final int tag)
    {
        if (position == UNKNOWN_INDEX)
        {
            // null because there's no actual field data at this point.
            acceptor.onError(PARSE_ERROR, messageType, tag, null);
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
