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
package uk.co.real_logic.otf_api;

import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.builder.DataDictionary;
import uk.co.real_logic.fix_gateway.builder.OrderSingleEncoder;
import uk.co.real_logic.fix_gateway.fields.DecimalFloat;
import uk.co.real_logic.fix_gateway.framer.session.InitiatorSession;

import static uk.co.real_logic.fix_gateway.flyweight_api.OrdType.Market;
import static uk.co.real_logic.fix_gateway.flyweight_api.Side.Sell;
import static uk.co.real_logic.fix_gateway.otf_api.OtfMessageAcceptor.NEW_ORDER_SINGLE;

/**
 * .
 */
public class SampleOtfMain
{
    public static void main(final String[] args) throws Exception
    {
        // Static configuration lasts the duration of a FIX-Gateway instance
        final StaticConfiguration configuration = new StaticConfiguration();

        // You register the acceptor - which is your custom application hook
        // Your generic acceptor then gets callbacks for each field of the tag or tags that it
        // gets registered for
        configuration.registerAcceptor(new SampleOtfAcceptor(), NEW_ORDER_SINGLE);

        try (final FixGateway gateway = FixGateway.launch(configuration))
        {
            // The data dictionary is generated from the standard FIX XML dictionary specification.
            final DataDictionary dictionary = new DataDictionary();

            // Each outbound session with an Exchange or broker is represented by
            // a Session object. Each session object can be configured with connection
            // details and credentials.
            final SessionConfiguration sessionConfig = SessionConfiguration.builder()
                .address("broker.example.com", 9999)
                .credentials("username", "password")
                .build();

            final InitiatorSession session = gateway.initiate(sessionConfig, dictionary);

            // Specific encoders are generated for each type of message
            // from the same dictionary as the decoders.
            final DecimalFloat price = new DecimalFloat(2000, 2);
            final DecimalFloat quantity = new DecimalFloat(10, 0);

            final OrderSingleEncoder orderSingle = dictionary.newOrderSingleEncoder();
            orderSingle.clOrdID("1")
                       .handlInst('1')
                       .ordType(Market)
                        // The API would follow a fluent style for setting up the different FIX message fields.
                       .side(Sell)
                       .symbol("MSFT")
                       .price(price)
                       .orderQty(quantity)
                       .transactTime(System.currentTimeMillis());

            // Having encoded the message, you can send it to the exchange via the session object.
            session.send(orderSingle);

            // If you want to produce multiple messages and rapidly fire them off then you just
            // need to update the fields in question and the other remain the side as your previous
            // usage.
            orderSingle.price(price.value(2010))
                       .orderQty(quantity.value(20));

            session.send(orderSingle);

            orderSingle.price(price.value(2020))
                       .orderQty(quantity.value(30));

            session.send(orderSingle);
        }
    }
}
