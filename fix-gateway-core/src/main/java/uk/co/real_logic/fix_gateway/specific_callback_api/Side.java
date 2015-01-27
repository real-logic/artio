package uk.co.real_logic.fix_gateway.specific_callback_api;

/**
 * .
 */
public enum Side
{
    Buy(1),
    Sell(2);

    private final int value;

    Side(int value)
    {
        this.value = value;
    }

    public int value()
    {
        return value;
    }

    public static Side valueOf(int value)
    {
        switch (value)
        {
            case 1:
                return Buy;
            case 2:
                return Sell;
            default:
                throw new IllegalArgumentException(value + " isn't a valid value");
        }
    }
}
