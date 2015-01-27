package uk.co.real_logic.specific_callback_api;

import uk.co.real_logic.fix_gateway.specific_callback_api.Acceptor;
import uk.co.real_logic.fix_gateway.specific_callback_api.MarketDataRequestDecoder;
import uk.co.real_logic.fix_gateway.specific_callback_api.OrderSingleDecoder;

/**
 * .
 */
public class SampleAcceptor implements Acceptor
{

    public void onOrderSingle(OrderSingleDecoder message, long sessionId)
    {

    }

    public void onMarketDataRequest(MarketDataRequestDecoder message, long sessionId)
    {

    }

}
