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
package uk.co.real_logic.reactive_api;

import uk.co.real_logic.fix_gateway.ValidationError;
import uk.co.real_logic.fix_gateway.reactive_api.*;

import static uk.co.real_logic.fix_gateway.flyweight_api.Side.Sell;

/**
 * .
 */
public class SampleReactiveAcceptor implements OrderSingleAcceptor, HeaderAcceptor
{

    private boolean wantsToSell;
    private String symbol;

    @Override
    public void onNext()
    {
        System.out.println("a NewOrderSingle has arrived");
    }

    @Override
    public void onMessageTypeField(int messageType)
    {

    }

    @Override
    public void onSenderCompIdField(AsciiFieldFlyweight senderCompId)
    {

    }

    @Override
    public void onSenderSubIdField(AsciiFieldFlyweight senderSubId)
    {

    }

    @Override
    public void onSenderLocationIdField(AsciiFieldFlyweight senderLocationId)
    {

    }

    @Override
    public void onClOrdIDField(AsciiFieldFlyweight clOrdID)
    {

    }

    @Override
    public void onHandlInstField(char handlInst)
    {

    }

    @Override
    public void onSideField(SideFlyweight side)
    {
        wantsToSell = side.getSide() == Sell;
    }

    @Override
    public void onPriceField(DecimalFloatFlyweight price)
    {

    }

    @Override
    public void onOrdTypeField(OrdTypeFlyweight ordType)
    {

    }

    @Override
    public void onTransactTimeField(long transactTime)
    {

    }

    @Override
    public void onSymbolField(AsciiFieldFlyweight symbol)
    {

    }

    @Override
    public void onComplete()
    {
        if (wantsToSell && "USD".equals(symbol))
        {
            System.out.println("Our client wants to sell dollars");
        }
    }

    @Override
    public boolean onError(ValidationError error, int messageType, int tagNumber, AsciiFieldFlyweight value)
    {
        System.err.println(error);
        return false;
    }
}
