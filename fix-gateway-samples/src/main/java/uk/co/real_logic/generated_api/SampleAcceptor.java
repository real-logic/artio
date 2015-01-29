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
package uk.co.real_logic.generated_api;

import uk.co.real_logic.fix_gateway.generated_api.Acceptor;
import uk.co.real_logic.fix_gateway.generated_api.MarketDataRequestDecoder;
import uk.co.real_logic.fix_gateway.generated_api.OrderSingleDecoder;
import uk.co.real_logic.fix_gateway.util.StringFlyweight;

import static uk.co.real_logic.fix_gateway.generated_api.Side.Sell;

/**
 * The acceptor is the callback for you to receive your application
 */
public class SampleAcceptor implements Acceptor
{

    public void onOrderSingle(final OrderSingleDecoder message, final long sessionId)
    {
        System.out.println("a NewOrderSingle has arrived");

        // Each field in the message is represented by a fluent decoder method.
        if (message.side() == Sell)
        {
            // Required fields can just be used directly
            if ("USD".equals(message.symbol()))
            {
                System.out.println("Our client wants to sell dollars");
            }
        }
        // Optional fields would all have boolean flags to indicate whether
        // They were present in the message that was decoded or not.
        else if(message.hasClientID())
        {
            final StringFlyweight clientID = message.clientID();
        }
    }

    public void onMarketDataRequest(final MarketDataRequestDecoder message, final long sessionId)
    {
        // You would implement a different callback to receive a different
        // message type
    }

    // ,..

}
