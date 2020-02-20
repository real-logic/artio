/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.artio.builder;

import org.agrona.DirectBuffer;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * .
 */
public class OrderSingleEncoder implements Encoder
{
    private AsciiBuffer clOrdID;
    private char handlInst;
    private Side side;
    private OrdType ordType;
    private long transactTime;

    private boolean orderQtyIsPresent;
    private DecimalFloat orderQty;

    private boolean priceIsPresent;
    private DecimalFloat price;

    public OrderSingleEncoder()
    {
    }

    public long encode(final MutableAsciiBuffer buffer, final int offset)
    {
        return 0;
    }

    public void reset()
    {

    }

    public OrderSingleEncoder clOrdID(final String clOrdID)
    {
        return this;
    }

    public OrderSingleEncoder clOrdID(final AsciiBuffer clOrdID)
    {
        return this;
    }

    public OrderSingleEncoder clOrdID(final DirectBuffer clOrdID, final int offset, final int length)
    {
        return this;
    }

    public OrderSingleEncoder handlInst(final char handlInst)
    {
        this.handlInst = handlInst;
        return this;
    }

    public OrderSingleEncoder side(final Side side)
    {
        this.side = side;
        return this;
    }

    public OrderSingleEncoder ordType(final OrdType ordType)
    {
        this.ordType = ordType;
        return this;
    }

    public OrderSingleEncoder transactTime(final long transactTime)
    {
        this.transactTime = transactTime;
        return this;
    }

    public OrderSingleEncoder symbol(final String symbol)
    {
        return this;
    }

    public OrderSingleEncoder price(final DecimalFloat price)
    {
        this.priceIsPresent = true;
        this.price = price;
        return this;
    }

    public OrderSingleEncoder orderQty(final DecimalFloat orderQty)
    {
        this.orderQtyIsPresent = true;
        this.orderQty = orderQty;
        return this;
    }

    public long messageType()
    {
        return 0L;
    }

    public SessionHeaderEncoder header()
    {
        return null;
    }

    public void resetMessage()
    {

    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        return builder;
    }
}
