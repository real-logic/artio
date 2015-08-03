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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import static uk.co.real_logic.fix_gateway.ValidationError.INVALID_CHECKSUM;
import static uk.co.real_logic.fix_gateway.ValidationError.PARSE_ERROR;
import static uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants.*;
import static uk.co.real_logic.fix_gateway.util.AsciiFlyweight.UNKNOWN_INDEX;

/**
 * Zero allocation generic parser for fix messages.
 *
 * Take care when refactoring:
 *
 * There are a lot of places where values are passed as parameters and not assigned to fields in order to
 * allow stack allocated primitives and avoid allocation.
 */
public final class OtfParser
{
    private static final int NO_CHECKSUM = 0;
    private static final int UNKNOWN = -1;

    private final AsciiFlyweight string = new AsciiFlyweight();
    private final AsciiFieldFlyweight stringField = new AsciiFieldFlyweight();

    private final OtfMessageAcceptor acceptor;
    private final IntDictionary groupToField;

    private int checksum;
    private int checksumOffset;
    private int messageType;
    private int tag;

    public OtfParser(final OtfMessageAcceptor acceptor, final IntDictionary groupToField)
    {
        this.acceptor = acceptor;
        this.groupToField = groupToField;
    }

    public void onMessage(final DirectBuffer buffer, final int offset, final int length)
    {
        string.wrap(buffer);
        acceptor.onNext();

        tag = UNKNOWN;
        this.messageType = UNKNOWN;

        checksum = NO_CHECKSUM;
        checksumOffset = 0;

        try
        {
            parseFields(buffer, offset, offset + length, UNKNOWN, null, 0);

            if (validChecksum(offset, checksum))
            {
                acceptor.onComplete();
            }
            else
            {
                invalidChecksum(this.messageType);
            }
        }
        catch (final Exception ex)
        {
            parseError(this.messageType, tag);
        }
    }

    private int parseFields(
        final DirectBuffer buffer,
        final int offset,
        final int end,
        final int groupTag,
        final IntHashSet groupFields,
        final int numberOfElementsInGroup)
    {
        int firstFieldInGroup = UNKNOWN;
        int indexOfGroupElement = 0;

        int position = offset;

        while (position < end)
        {
            final int equalsPosition = string.scan(position, end, '=');
            if (!validatePosition(equalsPosition, acceptor))
            {
                return position;
            }

            tag = string.getNatural(position, equalsPosition);
            final int valueOffset = equalsPosition + 1;
            final int endOfField = string.scan(valueOffset, end, START_OF_HEADER);
            if (!validatePosition(endOfField, acceptor))
            {
                return position;
            }

            final int valueLength = endOfField - valueOffset;

            final IntHashSet newGroupFields = groupToField.values(tag);
            if (newGroupFields == null)
            {
                if (insideAGroup(groupTag))
                {
                    if (isEndOfGroup(groupFields))
                    {
                        groupEnd(groupTag, numberOfElementsInGroup, indexOfGroupElement);
                        return position;
                    }
                    else
                    {
                        // First field first iteration
                        if (firstFieldInGroup == UNKNOWN)
                        {
                            firstFieldInGroup = tag;
                        }
                        // We've seen the first field again - its a new group iteration
                        else if (tag == firstFieldInGroup)
                        {
                            groupEnd(groupTag, numberOfElementsInGroup, indexOfGroupElement);
                            indexOfGroupElement++;
                            groupBegin(groupTag, numberOfElementsInGroup, indexOfGroupElement);
                        }
                    }
                }

                acceptor.onField(tag, buffer, valueOffset, valueLength);

                collectImportantFields(equalsPosition, valueOffset, endOfField, valueLength);

                position = endOfField + 1;
            }
            else
            {
                position = parseGroup(buffer, tag, valueOffset, endOfField, end, newGroupFields);
            }
        }

        return position;
    }

    private int parseGroup(
        final DirectBuffer buffer,
        final int tag,
        final int valueOffset,
        final int endOfField,
        final int end,
        final IntHashSet groupFields)
    {
        final int numberOfElements = string.getNatural(valueOffset, endOfField);

        acceptor.onGroupHeader(tag, numberOfElements);

        if (numberOfElements > 0)
        {
            groupBegin(tag, numberOfElements, 0);
            final int position = parseFields(buffer, endOfField + 1, end, tag, groupFields, numberOfElements);
            if (position == end)
            {
                groupEnd(tag, numberOfElements, numberOfElements - 1);
            }
            return position;
        }

        return endOfField;
    }

    private boolean isEndOfGroup(final IntHashSet groupFields)
    {
        return !groupFields.contains(tag);
    }

    private void collectImportantFields(
        final int equalsPosition,
        final int valueOffset,
        final int endOfField,
        final int valueLength)
    {
        if (tag == CHECKSUM)
        {
            checksum = string.getNatural(valueOffset, endOfField);
            checksumOffset = equalsPosition - 2;
        }
        else if (tag == MESSAGE_TYPE)
        {
            messageType = string.getMessageType(valueOffset, valueLength);
        }
    }

    private boolean insideAGroup(final int tag)
    {
        return tag != UNKNOWN;
    }

    private void groupBegin(final int tag, final int numberOfElements, final int index)
    {
        acceptor.onGroupBegin(tag, numberOfElements, index);
    }

    private void groupEnd(final int tag, final int numberOfElements, final int index)
    {
        acceptor.onGroupEnd(tag, numberOfElements, index);
    }

    private void parseError(final int messageType, final int tag)
    {
        acceptor.onError(PARSE_ERROR, messageType, tag, stringField);
    }

    private void invalidChecksum(final int messageType)
    {
        acceptor.onError(INVALID_CHECKSUM, messageType, CHECKSUM, stringField);
    }

    private boolean validatePosition(final int position, final OtfMessageAcceptor acceptor)
    {
        if (position == UNKNOWN_INDEX)
        {
            // null because there's no actual field data at this point.
            acceptor.onError(PARSE_ERROR, messageType, tag, null);

            return false;
        }

        return true;
    }

    private boolean validChecksum(final int offset, final int messageChecksum)
    {
        if (messageChecksum == NO_CHECKSUM)
        {
            return false;
        }

        final int correctChecksum = string.computeChecksum(offset, checksumOffset);
        return correctChecksum == messageChecksum;
    }
}
