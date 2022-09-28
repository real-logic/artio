/*
 * Copyright 2021 Monotonic Ltd.
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
package uk.co.real_logic.artio.system_tests;

import uk.co.real_logic.artio.CommonConfiguration;

import java.io.IOException;

public final class FixPExchangeExampleBuyer
{
    public static void main(final String[] args) throws IOException, InterruptedException
    {
        System.out.println("FixPExchangeApplication must be running in order for this example to work");
        System.out.println("Order can be printed using the vm option: -Dfix.core.debug=FIX_TEST");

        System.out.println("Starting ...");

        try (BinaryEntryPointClient client = new BinaryEntryPointClient(9999, new TestSystem(),
            CommonConfiguration.DEFAULT_ACCEPTOR_FIXP_KEEPALIVE_TIMEOUT_IN_MS))
        {
            client.writeNegotiate();
            client.readNegotiateResponse();

            client.writeEstablish();
            client.readEstablishAck(1, 0);

            int clOrdId = 1;
            while (true)
            {
                System.out.println("Sending order: " + clOrdId);
                client.writeNewOrderSingle(clOrdId);
                client.readExecutionReportNew(clOrdId);

                clOrdId++;

                Thread.sleep(200);
            }
        }
    }
}
