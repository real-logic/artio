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

import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.Session;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.builder.DataDictionary;
import uk.co.real_logic.fix_gateway.builder.OrderSingleEncoder;

import static uk.co.real_logic.fix_gateway.generated_api.OrdType.Market;
import static uk.co.real_logic.fix_gateway.generated_api.Side.Buy;
import static uk.co.real_logic.fix_gateway.generated_api.Side.Sell;

/**
 * .
 */
public class SampleMain
{
    public static void main(String[] args) throws Exception
    {
        final StaticConfiguration configuration = new StaticConfiguration();
        configuration.registerAcceptor(new SampleAcceptor());

        try(final FixGateway gateway = FixGateway.launch(configuration))
        {
            final DataDictionary dictionary = new DataDictionary();

            final SessionConfiguration sessionConfig = new SessionConfiguration()
                    .address("broker.example.com", 9999)
                    .credentials("username", "password");

            final Session session = gateway.initiate(sessionConfig, dictionary);

            final OrderSingleEncoder orderSingle = dictionary.newOrderSingleEncoder();
            orderSingle.clOrdID("1");
            orderSingle.handlInst('1');
            orderSingle.ordType(Market);
            orderSingle.side(Buy);
            orderSingle.symbol("MSFT");
            orderSingle.transactTime(System.currentTimeMillis());

            session.send(orderSingle);

            orderSingle.side(Sell);
            session.send(orderSingle);
        }
    }
}
