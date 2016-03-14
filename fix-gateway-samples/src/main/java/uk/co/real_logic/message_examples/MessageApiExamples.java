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
package uk.co.real_logic.message_examples;

import uk.co.real_logic.fix_gateway.builder.OrderSingleEncoder;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.fields.DecimalFloat;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.library.session.Session;
import uk.co.real_logic.fix_gateway.library.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.MessageValidationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.SenderCompIdValidationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.TargetCompIdValidationStrategy;

import static java.util.Arrays.asList;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.backoffIdleStrategy;
import static uk.co.real_logic.fix_gateway.builder.OrdType.Market;
import static uk.co.real_logic.fix_gateway.builder.Side.Sell;

/**
 * Example of what sending an OrderSingle message would be like using the API.
 */
public final class MessageApiExamples
{

    public static final String TARGET_COMP_ID = "targetCompId";
    public static final String SENDER_COMP_ID = "senderCompId";
    public static final String AERON_CHANNEL = "ipc:9999";

    public static void main(String[] args) throws Exception
    {
        // Static configuration lasts the duration of a FIX-Gateway instance
        final EngineConfiguration configuration = new EngineConfiguration()
            .aeronChannel(AERON_CHANNEL);

        try (final FixEngine gateway = FixEngine.launch(configuration))
        {
            final MessageValidationStrategy validationStrategy = new TargetCompIdValidationStrategy(TARGET_COMP_ID)
                .and(new SenderCompIdValidationStrategy(asList(SENDER_COMP_ID)));

            final AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.of(validationStrategy);

            final LibraryConfiguration libraryConfiguration = new LibraryConfiguration()
                .aeronChannel(AERON_CHANNEL)
                .messageValidationStrategy(validationStrategy)
                .authenticationStrategy(authenticationStrategy);

            try (final FixLibrary library = FixLibrary.connect(libraryConfiguration))
            {
                // Each outbound session with an Exchange or broker is represented by
                // a Session object. Each session object can be configured with connection
                // details and credentials.
                final SessionConfiguration sessionConfig = SessionConfiguration.builder()
                    .address("broker.example.com", 9999)
                    .credentials("username", "password")
                    .senderCompId(SENDER_COMP_ID)
                    .targetCompId(TARGET_COMP_ID)
                    .build();

                final Session session = library.initiate(sessionConfig, backoffIdleStrategy());

                // Specific encoders are generated for each type of message
                // from the same dictionary as the decoders.
                final DecimalFloat price = new DecimalFloat(2000, 2);
                final DecimalFloat quantity = new DecimalFloat(10, 0);

                final OrderSingleEncoder orderSingle = new OrderSingleEncoder();
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
}
