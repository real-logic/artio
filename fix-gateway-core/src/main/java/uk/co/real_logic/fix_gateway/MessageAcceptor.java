package uk.co.real_logic.fix_gateway;

/**
 * .
 */
public interface MessageAcceptor extends ErrorAcceptor
{
    void onNext();

    void onComplete();
}
