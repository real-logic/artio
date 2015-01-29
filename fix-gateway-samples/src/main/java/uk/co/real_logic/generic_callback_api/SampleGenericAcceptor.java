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
package uk.co.real_logic.generic_callback_api;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.generic_callback_api.FixMessageAcceptor;

public class SampleGenericAcceptor implements FixMessageAcceptor
{

    private boolean wantsToSell;
    private String symbol;

    @Override
    public void onStartMessage()
    {
        System.out.println("a NewOrderSingle has arrived");
    }

    @Override
    public void onField(final int tag, final DirectBuffer buffer, final int offset, final int length)
    {
        switch (tag)
        {
            // You switch on the tag in order to identify what field data has arrived.
            case SIDE:
                wantsToSell = buffer.getByte(offset) == SELL;
                break;

            case SYMBOL:
                symbol = buffer.getStringUtf8(offset, length);
                break;

            // Implement other tags in order to
            // Optional fields will either generate callbacks or not depending upon whether they
            // are present in the message.
        }
    }

    @Override
    public void onEndMessage(final boolean passedChecksum)
    {
        // Message has been parsed and passed its checksum check.
        // Now we can make a decision about what to do with the message.

        if (passedChecksum && wantsToSell && "USB".equals(symbol))
        {
            System.out.println("Our client wants to sell dollars");
        }
    }


    @Override
    public void onGroupBegin(final int tag, final int numberOfElements)
    {
        // Some FIX fields consist of repeating groups, you get callbacks
        // when these start and end.
    }

    @Override
    public void onGroupEnd(final int tag)
    {

    }
}
