package uk.co.real_logic.fix_gateway.reactive_api;

import uk.co.real_logic.fix_gateway.parser.DecimalFloat;

/**
 * .
 */
public interface DecimalFloatFlyweight extends AsciiFieldFlyweight
{
    void getFloat(DecimalFloat value);
}
