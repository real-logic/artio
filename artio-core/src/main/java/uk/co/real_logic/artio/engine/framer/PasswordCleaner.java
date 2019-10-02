/*
 * Copyright 2019 Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.engine.framer;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.ValidationError;
import uk.co.real_logic.artio.dictionary.IntDictionary;
import uk.co.real_logic.artio.fields.AsciiFieldFlyweight;
import uk.co.real_logic.artio.otf.MessageControl;
import uk.co.real_logic.artio.otf.OtfMessageAcceptor;
import uk.co.real_logic.artio.otf.OtfParser;
import uk.co.real_logic.artio.util.AsciiBuffer;

public class PasswordCleaner
{
    private static final int NO_ENTRY = -1;
    private static final int REPLACEMENT_LENGTH = 3;
    private static final UnsafeBuffer REPLACEMENT = new UnsafeBuffer(new byte[REPLACEMENT_LENGTH]);
    static
    {
        REPLACEMENT.putStringWithoutLengthAscii(0, "***");
    }

    private int passwordOffset;
    private int passwordLength;
    private int bodyLength;
    private int bodyLengthOffset;
    private int lengthOfBodyLength;

    private final FieldScanner fieldScanner = new FieldScanner();
    private final OtfParser parser = new OtfParser(fieldScanner, new IntDictionary());
    private final ExpandableArrayBuffer cleanedBuffer = new ExpandableArrayBuffer();
    private int cleanedLength;

    public void clean(final DirectBuffer buffer, final int offset, final int length)
    {
        parser.onMessage(buffer, offset, length);

        if (passwordOffset == NO_ENTRY)
        {
            cleanedBuffer.putBytes(0, buffer, offset, length);
            cleanedLength = length;
        }
        else
        {
            final int headLength = passwordOffset - offset;
            cleanedBuffer.putBytes(0, buffer, offset, headLength);

            cleanedBuffer.putBytes(headLength, REPLACEMENT, 0, REPLACEMENT_LENGTH);

            final int tailOffset = passwordOffset + passwordLength;
            final int tailLength = length - (headLength + passwordLength);
            cleanedBuffer.putBytes(headLength + REPLACEMENT_LENGTH, buffer, tailOffset, tailLength);

            cleanedLength = length - passwordLength + REPLACEMENT_LENGTH;

            final int newBodyLength = bodyLength - passwordLength + REPLACEMENT_LENGTH;
            final int relativeBodyLengthOffset = bodyLengthOffset - offset;
            cleanedBuffer.putNaturalIntAsciiFromEnd(
                newBodyLength, relativeBodyLengthOffset + lengthOfBodyLength);
        }
    }

    public DirectBuffer cleanedBuffer()
    {
        return cleanedBuffer;
    }

    public int cleanedLength()
    {
        return cleanedLength;
    }

    private class FieldScanner implements OtfMessageAcceptor
    {
        public MessageControl onNext()
        {
            passwordOffset = NO_ENTRY;
            bodyLength = NO_ENTRY;
            bodyLengthOffset = NO_ENTRY;
            lengthOfBodyLength = NO_ENTRY;
            return MessageControl.CONTINUE;
        }

        public MessageControl onField(final int tag, final AsciiBuffer buffer, final int offset, final int length)
        {
            switch (tag)
            {
                case Constants.PASSWORD:
                    passwordOffset = offset;
                    passwordLength = length;
                    break;

                case Constants.BODY_LENGTH:
                    bodyLengthOffset = offset;
                    lengthOfBodyLength = length;
                    bodyLength = buffer.getInt(offset, offset + length);
                    break;
            }
            return MessageControl.CONTINUE;
        }

        public MessageControl onGroupHeader(final int tag, final int numInGroup)
        {
            return MessageControl.CONTINUE;
        }

        public MessageControl onGroupBegin(final int tag, final int numInGroup, final int index)
        {
            return MessageControl.CONTINUE;
        }

        public MessageControl onGroupEnd(final int tag, final int numInGroup, final int index)
        {
            return MessageControl.CONTINUE;
        }

        public MessageControl onComplete()
        {
            return MessageControl.CONTINUE;
        }

        public boolean onError(
            final ValidationError error,
            final int messageType,
            final int tagNumber,
            final AsciiFieldFlyweight value)
        {
            return false;
        }

    }
}
