/**
 * Strawman API proposal where:
 *
 * There is a single generic handler interface to be called for any type of artio message.
 *
 * This handler could be registered against different types of Fix messages.
 *
 * Pros: simple, generic and flexible
 * Cons: not semantic, nearly all call sites megamorphic in practice, potentially slower
 */
package uk.co.real_logic.artio.otf;
