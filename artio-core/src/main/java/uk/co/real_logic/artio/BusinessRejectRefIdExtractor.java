/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio;

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.dictionary.LongDictionary;
import uk.co.real_logic.artio.dictionary.SessionConstants;
import uk.co.real_logic.artio.fields.AsciiFieldFlyweight;
import uk.co.real_logic.artio.otf.MessageControl;
import uk.co.real_logic.artio.otf.OtfMessageAcceptor;
import uk.co.real_logic.artio.otf.OtfParser;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static uk.co.real_logic.artio.util.MessageTypeEncoding.packMessageType;

public class BusinessRejectRefIdExtractor implements OtfMessageAcceptor
{
    private static final MutableAsciiBuffer NO_RESULT = new MutableAsciiBuffer(new byte[0]);

    private static final long INDICATION_OF_INTEREST = packMessageType("6");

    private static final int IOI_ID = 23;

    private final OtfParser parser = new OtfParser(this, new LongDictionary());

    private AsciiBuffer buffer;
    private int offset;
    private int length;
    private int sequenceNumber;

    private int searchTag;

    public void search(final long msgType, final DirectBuffer msgBuffer, final int msgOffset, final int msgLength)
    {
        buffer = NO_RESULT;
        offset = 0;
        length = 0;
        sequenceNumber = 0;

        if (msgType == INDICATION_OF_INTEREST)
        {
            searchTag = IOI_ID;
        }

        parser.onMessage(msgBuffer, msgOffset, msgLength);
    }

    public MessageControl onNext()
    {
        return MessageControl.CONTINUE;
    }

    public MessageControl onComplete()
    {
        return MessageControl.CONTINUE;
    }

    public MessageControl onField(final int tag, final AsciiBuffer buffer, final int offset, final int length)
    {
        if (tag == SessionConstants.MSG_SEQ_NO)
        {
            sequenceNumber = buffer.getInt(offset, offset + length);
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

    public boolean onError(
        final ValidationError error, final long messageType, final int tagNumber, final AsciiFieldFlyweight value)
    {
        return false;
    }

    public AsciiBuffer buffer()
    {
        return buffer;
    }

    public int offset()
    {
        return offset;
    }

    public int length()
    {
        return length;
    }

    public int sequenceNumber()
    {
        return sequenceNumber;
    }
}
