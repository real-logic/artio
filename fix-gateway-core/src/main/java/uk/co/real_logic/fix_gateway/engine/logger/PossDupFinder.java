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
package uk.co.real_logic.fix_gateway.engine.logger;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.ValidationError;
import uk.co.real_logic.fix_gateway.decoder.Constants;
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.otf.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

class PossDupFinder implements OtfMessageAcceptor
{
    public static final int NO_ENTRY = -1;

    private final AsciiBuffer ascii = new MutableAsciiBuffer();

    private int possDupOffset;
    private int sendingTimeEnd;
    private int bodyLength;
    private int bodyLengthOffset;
    private int lengthOfBodyLength;

    public void onNext()
    {
        possDupOffset = NO_ENTRY;
        sendingTimeEnd = NO_ENTRY;
        bodyLength = NO_ENTRY;
        bodyLengthOffset = NO_ENTRY;
        lengthOfBodyLength = NO_ENTRY;
    }

    public void onField(final int tag, final DirectBuffer buffer, final int offset, final int length)
    {
        switch (tag)
        {
            case Constants.POSS_DUP_FLAG:
                possDupOffset = offset;
                break;

            case Constants.SENDING_TIME:
                sendingTimeEnd = offset + length + 1;
                break;

            case Constants.BODY_LENGTH:
                ascii.wrap(buffer);
                bodyLengthOffset = offset;
                lengthOfBodyLength = length;
                bodyLength = ascii.getInt(offset, offset + length);
                break;
        }
    }

    public void onGroupHeader(final int tag, final int numInGroup)
    {

    }

    public void onGroupBegin(final int tag, final int numInGroup, final int index)
    {

    }

    public void onGroupEnd(final int tag, final int numInGroup, final int index)
    {

    }

    public void onComplete()
    {

    }

    public boolean onError(final ValidationError error,
                           final int messageType,
                           final int tagNumber,
                           final AsciiFieldFlyweight value)
    {
        return false;
    }

    public int possDupOffset()
    {
        return possDupOffset;
    }

    public int sendingTimeEnd()
    {
        return sendingTimeEnd;
    }

    public int bodyLength()
    {
        return bodyLength;
    }

    public int bodyLengthOffset()
    {
        return bodyLengthOffset;
    }

    public int lengthOfBodyLength()
    {
        return lengthOfBodyLength;
    }
}
