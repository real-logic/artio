package uk.co.real_logic.specific_callback_api;

import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.Session;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.builder.DataDictionary;
import uk.co.real_logic.fix_gateway.builder.OrderSingleEncoder;

import static uk.co.real_logic.fix_gateway.specific_callback_api.OrdType.Market;
import static uk.co.real_logic.fix_gateway.specific_callback_api.Side.Buy;
import static uk.co.real_logic.fix_gateway.specific_callback_api.Side.Sell;

/**
 * .
 */
public class SampleMain
{
    public static void main(String[] args) throws Exception
    {
        StaticConfiguration configuration = new StaticConfiguration();
        configuration.registerAcceptor(new SampleAcceptor());

        try(FixGateway gateway = FixGateway.launch(configuration))
        {
            DataDictionary dictionary = new DataDictionary();

            SessionConfiguration sessionConfig = new SessionConfiguration()
                    .address("broker.example.com", 9999)
                    .credentials("username", "password");

            Session session = gateway.initiate(sessionConfig, dictionary);

            OrderSingleEncoder orderSingle = dictionary.newOrderSingleEncoder();
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
