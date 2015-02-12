package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.fix_gateway.reactive_api.AsciiFieldFlyweight;

/**
 * .
 */
@FunctionalInterface
public interface ErrorAcceptor
{
    boolean onError(ValidationError error, int messageType, int tagNumber, AsciiFieldFlyweight value);
}
