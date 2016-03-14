/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.util;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.fields.DecimalFloat;

/**
 * Mutable String class that flyweights a data buffer. This assumes a US-ASCII encoding
 * and should only be used for performance sensitive decoding/encoding tasks.
 */
public interface AsciiBuffer extends DirectBuffer
{
    int UNKNOWN_INDEX = -1;
    byte YES = 'Y';
    byte NEGATIVE = '-';

    int getNatural(final int startInclusive, final int endExclusive);

    int getInt(int startInclusive, final int endExclusive);

    int getDigit(final int index);

    boolean isDigit(final int index);

    byte getByte(final int index);

    char getChar(final int index);

    boolean getBoolean(final int index);

    byte[] getBytes(final byte[] oldBuffer, final int offset, final int length);

    char[] getChars(final char[] oldBuffer, final int offset, final int length);

    /**
     * May not  be a performant conversion: don't use this on a critical application path.
     *
     * @param offset
     * @param length
     * @return a String
     */
    String getAscii(final int offset, final int length);

    int getMessageType(final int offset, final int length);

    DecimalFloat getFloat(final DecimalFloat number, int offset, int length);

    int getLocalMktDate(final int offset, final int length);

    long getUtcTimestamp(final int offset, final int length);

    long getUtcTimeOnly(final int offset, final int length);

    int getUtcDateOnly(final int offset);

    int scanBack(final int startInclusive, final int endExclusive, final char terminatingCharacter);

    int scanBack(final int startInclusive, final int endExclusive, final byte terminator);

    int scan(final int startInclusive, final int endInclusive, final char terminatingCharacter);

    int scan(final int startInclusive, final int endInclusive, final byte terminator);

    int computeChecksum(final int offset, final int end);
}
