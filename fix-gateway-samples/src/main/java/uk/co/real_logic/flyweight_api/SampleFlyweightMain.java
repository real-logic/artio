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
package uk.co.real_logic.flyweight_api;

import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.StaticConfiguration;

/**
 * .
 */
public class SampleFlyweightMain
{
    public static void main(String[] args) throws Exception
    {
        // Static configuration lasts the duration of a FIX-Gateway instance
        final StaticConfiguration configuration = new StaticConfiguration();

        // You register the acceptor - which is your custom application hook
        // that receives messages from external connections.
        configuration.registerAcceptor(new SampleFlyweightOrderSingleAcceptor(), (error, msgType, tag, value) ->
        {
            System.err.println(error);
            return false;
        });

        try(final FixGateway gateway = FixGateway.launch(configuration))
        {
            // This would be the same as the SampleOtfMain sample code for sending a message.
        }
    }
}
