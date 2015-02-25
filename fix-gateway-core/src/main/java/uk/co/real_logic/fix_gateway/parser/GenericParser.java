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
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.framer.MessageHandler;
import uk.co.real_logic.fix_gateway.otf_api.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;
import uk.co.real_logic.fix_gateway.util.IntHashSet;

import static uk.co.real_logic.fix_gateway.ValidationError.INVALID_CHECKSUM;
import static uk.co.real_logic.fix_gateway.ValidationError.PARSE_ERROR;
import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.*;
import static uk.co.real_logic.fix_gateway.util.AsciiFlyweight.UNKNOWN_INDEX;

// TODO: what should we do if the callbacks throw an exception?

/**
 * Zero allocation generic parser for fix messages.
 *
 * Take care when refactoring:
 *
 * There are a lot of places where values are passed as parameters and not assigned to fields in order to
 * allow stack allocated primitives and avoid allocation.
 */
public final class GenericParser implements MessageHandler
{
    private static final int NO_CHECKSUM = 0;
    private static final int UNKNOWN = -1;

    private final AsciiFlyweight string = new AsciiFlyweight(null);
    private final AsciiFieldFlyweight stringField = new AsciiFieldFlyweight();

    private final OtfMessageAcceptor acceptor;
    private final IntDictionary groupToField;

    private int checksum;
    private int checksumOffset;
    private int messageType;
    private int tag;

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

        parseMessage(buffer, offset, end, new GroupInformation());
    }

    private void parseMessage(final DirectBuffer buffer, final int offset, final int end, final GroupInformation currentGroup)
    {
        tag = UNKNOWN;
        messageType = UNKNOWN;
        checksum = NO_CHECKSUM;
        checksumOffset = 0;
        try
        {
            parseFields(buffer, offset, end, currentGroup);
        }
        catch (final IllegalArgumentException ex)
        {
            // Error parsing the message
            // ex.printStackTrace();
            onParseError(messageType, tag);
        }

        if (validChecksum(buffer, offset, checksumOffset, checksum))
        {
            acceptor.onComplete();
        }
        else
        {
            onInvalidChecksum(messageType);
        }
    }

    private int parseFields(final DirectBuffer buffer, final int offset, final int end, GroupInformation currentGroup)
    {
        int position = offset;

        while (position < end)
        {
            final int beginningOfField = position;
            final int equalsPosition = string.scan(position, end, '=');
            if (!validatePosition(equalsPosition, acceptor, messageType, tag))
            {
                return beginningOfField;
            }

            tag = string.getInt(position, equalsPosition);
            final int valueOffset = equalsPosition + 1;
            final int endOfField = string.scan(valueOffset, end, START_OF_HEADER);
            if (!validatePosition(endOfField, acceptor, messageType, tag))
            {
                return beginningOfField;
            }

            final int valueLength = endOfField - valueOffset;

            final IntHashSet newGroupFields = groupToField.values(tag);
            if (newGroupFields == null)
            {
                if (insideAGroup(currentGroup.groupTag))
                {
                    // Non-group field means end of group
                    if (!currentGroup.groupFields.contains(tag))
                    {
                        onGroupEnd(currentGroup.groupTag, currentGroup.numberOfElementsInGroup, currentGroup.indexOfGroupElement);
                        return beginningOfField;
                    }
                    else
                    {
                        // First field first iteration
                        if (currentGroup.firstFieldInGroup == UNKNOWN)
                        {
                            currentGroup.firstFieldInGroup = tag;
                        }
                        // We've seen the first field again - its a new group iteration
                        else if(tag == currentGroup.firstFieldInGroup)
                        {
                            onGroupEnd(currentGroup.groupTag, currentGroup.numberOfElementsInGroup, currentGroup.indexOfGroupElement);
                            currentGroup.indexOfGroupElement++;
                            onGroupBegin(currentGroup.groupTag, currentGroup.numberOfElementsInGroup, currentGroup.indexOfGroupElement);
                        }
                    }
                }

                acceptor.onField(tag, buffer, valueOffset, valueLength);

                storeImportantFields(equalsPosition, valueOffset, endOfField, valueLength);

                position = endOfField + 1;
            }
            else
            {
                position = parseGroup(buffer, tag, valueOffset, endOfField, end, newGroupFields);
            }
        }

        return position;
    }

    private void storeImportantFields(final int equalsPosition, final int valueOffset, final int endOfField, final int valueLength)
    {
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

    private int parseGroup(
            final DirectBuffer buffer,
            final int tag,
            final int valueOffset,
            final int endOfField,
            final int end,
            final IntHashSet newGroupFields)
    {
        final int numberOfElements = string.getInt(valueOffset, endOfField);

        acceptor.onGroupHeader(tag, numberOfElements);

        if (numberOfElements > 0)
        {
            onGroupBegin(tag, numberOfElements, 0);
            final GroupInformation info = new GroupInformation();
            info.groupTag = tag;
            info.firstFieldInGroup = UNKNOWN;
            info.groupFields = newGroupFields;
            info.numberOfElementsInGroup = numberOfElements;
            info.indexOfGroupElement = 0;
            final int position = parseFields(buffer, endOfField + 1, end, info);
            if (position == end)
            {
                onGroupEnd(tag, numberOfElements, numberOfElements - 1);
            }
            return position;
        }

        return endOfField;
    }

    private boolean insideAGroup(final int tag)
    {
        return tag != UNKNOWN;
    }

    private void onGroupBegin(final int tag, final int numberOfElements, final int index)
    {
        acceptor.onGroupBegin(tag, numberOfElements, index);
    }

    private void onGroupEnd(final int tag, final int numberOfElements, final int index)
    {
        acceptor.onGroupEnd(tag, numberOfElements, index);
    }

    private void onParseError(final int messageType, final int tag)
    {
        acceptor.onError(PARSE_ERROR, messageType, tag, stringField);
    }

    private void onInvalidChecksum(final int messageType)
    {
        acceptor.onError(INVALID_CHECKSUM, messageType, CHECKSUM, stringField);
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

    private boolean validChecksum(final DirectBuffer buffer, final int offset, final int length, final int checksum)
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

    private static class GroupInformation
    {
        int groupTag = UNKNOWN;
        IntHashSet groupFields = null;
        int firstFieldInGroup;
        int numberOfElementsInGroup;
        int indexOfGroupElement;
    }
}
