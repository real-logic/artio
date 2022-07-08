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

import org.hamcrest.Matchers;
import uk.co.real_logic.artio.OrdType;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.builder.NewOrderSingleEncoder;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.session.Session;

import static org.hamcrest.MatcherAssert.assertThat;

final class OrderFactory
{
    private static NewOrderSingleEncoder makeOrder()
    {
        final NewOrderSingleEncoder newOrderSingle = new NewOrderSingleEncoder();
        final DecimalFloat price = new DecimalFloat(100);
        final DecimalFloat orderQty = new DecimalFloat(2);
        final UtcTimestampEncoder transactTime = new UtcTimestampEncoder();

        final int transactTimeLength = transactTime.encode(System.currentTimeMillis());

        newOrderSingle
            .clOrdID("A")
            .side(Side.BUY)
            .transactTime(transactTime.buffer(), transactTimeLength)
            .ordType(OrdType.MARKET)
            .price(price);

        newOrderSingle.instrument().symbol("MSFT");
        newOrderSingle.orderQtyData().orderQty(orderQty);

        return newOrderSingle;
    }

    static void sendOrder(final Session session)
    {
        final long position = session.trySend(makeOrder());
        assertThat(position, Matchers.greaterThan(0L));
    }

    static void sendOrder(final FixConnection connection)
    {
        final int msgSeqNum = connection.acquireMsgSeqNum();
        final NewOrderSingleEncoder encoder = makeOrder();
        connection.setupHeader(encoder.header(), msgSeqNum, false);
        connection.send(encoder);
    }
}
