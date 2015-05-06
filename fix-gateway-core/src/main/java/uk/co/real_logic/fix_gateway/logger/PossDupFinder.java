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
package uk.co.real_logic.fix_gateway.logger;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.ValidationError;
import uk.co.real_logic.fix_gateway.decoder.Constants;
import uk.co.real_logic.fix_gateway.fields.AsciiFieldFlyweight;
import uk.co.real_logic.fix_gateway.otf.OtfMessageAcceptor;

class PossDupFinder implements OtfMessageAcceptor
{
    public static final int NO_ENTRY = -1;

    private int possDupOffset;
    private int sendingTimeOffset;

    public void onNext()
    {
        possDupOffset = NO_ENTRY;
        sendingTimeOffset = NO_ENTRY;
    }

    public void onField(final int tag, final DirectBuffer buffer, final int offset, final int length)
    {
        if (tag == Constants.POSS_DUP_FLAG)
        {
            possDupOffset = offset;
        }
        else if (tag == Constants.SENDING_TIME)
        {
            sendingTimeOffset = offset;
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

    public int sendingTimeOffset()
    {
        return sendingTimeOffset;
    }
}
