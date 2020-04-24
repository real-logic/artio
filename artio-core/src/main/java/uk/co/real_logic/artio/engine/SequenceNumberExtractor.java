/*
 * Copyright 2019 Monotonic Ltd.
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
package uk.co.real_logic.artio.engine;

import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import uk.co.real_logic.artio.ValidationError;
import uk.co.real_logic.artio.dictionary.LongDictionary;
import uk.co.real_logic.artio.fields.AsciiFieldFlyweight;
import uk.co.real_logic.artio.otf.MessageControl;
import uk.co.real_logic.artio.otf.OtfMessageAcceptor;
import uk.co.real_logic.artio.otf.OtfParser;
import uk.co.real_logic.artio.util.AsciiBuffer;

import static uk.co.real_logic.artio.dictionary.SessionConstants.*;
import static uk.co.real_logic.artio.otf.MessageControl.CONTINUE;
import static uk.co.real_logic.artio.otf.MessageControl.STOP;

public class SequenceNumberExtractor
{
    public static final int NO_SEQUENCE_NUMBER = -1;

    private final ErrorHandler errorHandler;

    private boolean isSequenceReset;
    private int sequenceNumber;

    public SequenceNumberExtractor(final ErrorHandler errorHandler)
    {
        this.errorHandler = errorHandler;
    }

    public int extract(
        final DirectBuffer buffer, final int offset, final int length)
    {
        sequenceNumber = NO_SEQUENCE_NUMBER;
        isSequenceReset = false;

        parser.onMessage(buffer, offset, length);

        return sequenceNumber;
    }

    private final OtfMessageAcceptor extractor = new OtfMessageAcceptor()
    {
        public MessageControl onNext()
        {
            return CONTINUE;
        }

        public MessageControl onComplete()
        {
            return CONTINUE;
        }

        public MessageControl onField(
            final int tag, final AsciiBuffer buffer, final int offset, final int length)
        {
            if (tag == MESSAGE_TYPE)
            {
                isSequenceReset = length == 1 && buffer.getByte(offset) == SEQUENCE_RESET_TYPE_BYTE;
            }
            else if (tag == MSG_SEQ_NO && !isSequenceReset)
            {
                sequenceNumber = buffer.getInt(offset, offset + length);
                return STOP;
            }
            else if (tag == NEW_SEQ_NO && isSequenceReset)
            {
                sequenceNumber = buffer.getInt(offset, offset + length) - 1;
                return STOP;
            }

            return CONTINUE;
        }

        public MessageControl onGroupHeader(final int tag, final int numInGroup)
        {
            return CONTINUE;
        }

        public MessageControl onGroupBegin(final int tag, final int numInGroup, final int index)
        {
            return CONTINUE;
        }

        public MessageControl onGroupEnd(final int tag, final int numInGroup, final int index)
        {
            return CONTINUE;
        }

        public boolean onError(
            final ValidationError error,
            final long messageType,
            final int tagNumber,
            final AsciiFieldFlyweight value)
        {
            return false;
        }
    };

    private final OtfParser parser = new OtfParser(extractor, new LongDictionary());
}
