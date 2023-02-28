/*
 * Copyright 2015-2023 Real Logic Limited.
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
package uk.co.real_logic.artio.fields;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;


import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;

public class DecimalFloatTest
{
    private static final DecimalFloat ZERO = new DecimalFloat(0, 0);
    private static final DecimalFloat FIVE = new DecimalFloat(5, 0);
    private static final DecimalFloat MINUS_FIVE = new DecimalFloat(-5, 0);

    private static final DecimalFloat POINT_ONE = new DecimalFloat(1, 1);
    private static final DecimalFloat FIVE_POINT_FIVE = new DecimalFloat(55, 1);
    private static final DecimalFloat MINUS_FIVE_POINT_FIVE = new DecimalFloat(-55, 1);

    @Test
    public void compareToDetectsEqualIntegers()
    {
        assertThat(ZERO, comparesEqualTo(ZERO));
        assertThat(FIVE, comparesEqualTo(new DecimalFloat(5, 0)));
        assertThat(MINUS_FIVE, comparesEqualTo(new DecimalFloat(-5, 0)));

        assertThat(new DecimalFloat(54321, 3), comparesEqualTo(new DecimalFloat(543210, 4)));
        assertThat(new DecimalFloat(543210, 4), comparesEqualTo(new DecimalFloat(54321, 3)));
    }

    @Test
    public void compareToOrdersIntegers()
    {
        assertOrderWithNegatives(ZERO, FIVE);
        assertOrderWithNegatives(MINUS_FIVE, FIVE);
    }

    @Test
    public void compareToOrdersFloatsOfSameScale()
    {
        assertOrderWithNegatives(POINT_ONE, FIVE_POINT_FIVE);
        assertOrderWithNegatives(MINUS_FIVE_POINT_FIVE, POINT_ONE);
        assertOrderWithNegatives(MINUS_FIVE_POINT_FIVE, FIVE);
    }

    @Test
    public void compareToOrdersFloatsWithIntegers()
    {
        assertOrderWithNegatives(ZERO, POINT_ONE);
        assertOrderWithNegatives(MINUS_FIVE_POINT_FIVE, ZERO);
        assertOrderWithNegatives(MINUS_FIVE_POINT_FIVE, FIVE);
    }

    @Test
    public void compareToOrdersFloatsOfDifferentScale()
    {
        assertOrderWithNegatives(new DecimalFloat(45, 2), new DecimalFloat(45, 1));
        assertOrderWithNegatives(new DecimalFloat(9, 2), POINT_ONE);
    }

    @Test
    public void compareToOrdersFloatsOfDifferentScaleVsZero()
    {
        assertOrderWithNegatives(ZERO, new DecimalFloat(45, 1));
        assertOrderWithNegatives(ZERO, new DecimalFloat(45, 2));
    }

    @Test
    public void compareToOrderFloatsOfDifferentScaleWithMultiDigitValues()
    {
        assertOrderWithNegatives(new DecimalFloat(54321, 2), new DecimalFloat(543219, 3));
        assertOrderWithNegatives(new DecimalFloat(54321, 2), new DecimalFloat(5433, 1));
    }

    @Test
    public void normaliseValuesDuringConstruction()
    {
        assertThat(new DecimalFloat(0, 0), equalTo(new DecimalFloat(0, 0)));
        assertThat(new DecimalFloat(0, 0), equalTo(new DecimalFloat(0, 25)));
        assertThat(new DecimalFloat(0, 0), equalTo(new DecimalFloat(0, -25)));
        assertThat(new DecimalFloat(5000, 0), equalTo(new DecimalFloat(500000, 2)));
        assertThat(new DecimalFloat(5000, 0), equalTo(new DecimalFloat(50, -2)));
        assertThat(new DecimalFloat(1234, 2), equalTo(new DecimalFloat(123400, 4)));
    }

    @Test(expected = NumberFormatException.class)
    public void shouldNotConvertInvalidStringIntoANumber()
    {
        new DecimalFloat().fromString("ABC");
    }

    @Test(expected = ArithmeticException.class)
    public void shouldNotParseValueOutOfRange()
    {
        // Valid decimal floats have max 18 digits, could also have e, E, -, + or .
        new DecimalFloat().fromString("10000000000000000000000");
    }

    // Bug reproduction testcase
    @Test(expected = ArithmeticException.class)
    public void shouldNotParseOverflowingValue()
    {
        new DecimalFloat().fromString("99999999999999990000000");
    }

    @Test
    public void parseZeroDecimalFloat()
    {
        assertThat(new DecimalFloat(0, 0), equalTo(new DecimalFloat().fromString("0")));
    }

    // Bug reproduction testcase
    @Test(expected = ArithmeticException.class)
    public void shouldNotDecodeOverflowingValue()
    {
        parseNumberFromBuffer("99999999999999990000000");
    }

    @Test(expected = ArithmeticException.class)
    public void shouldNotDecodeValueOutOfRange()
    {
        parseNumberFromBuffer("10000000000000000000000");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotEncodeAnInvalidValue()
    {
        final MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[1000]);
        buffer.putFloatAscii(0, DecimalFloat.NAN);
    }

    @Test
    public void shouldNotBeAbleToRedefineConstantValues()
    {
        final DecimalFloat value;
        final DecimalFloat zero = DecimalFloat.ZERO.mutableCopy();
        value = DecimalFloat.ZERO.mutableCopy(); // compiler now prevent from modifying constants such as ZERO
        assertThat(value, equalTo(zero));
        assertThat(DecimalFloat.ZERO, equalTo(zero));

        // When
        value.set(new DecimalFloat(5));

        // Then
        assertThat(value, equalTo(new DecimalFloat(5)));
        assertThat(DecimalFloat.ZERO, equalTo(zero));
    }

    private void parseNumberFromBuffer(final String number)
    {
        final AsciiBuffer buffer = new MutableAsciiBuffer(number.getBytes(US_ASCII));
        buffer.getFloat(new DecimalFloat(), 0, buffer.capacity());
    }

    private void assertOrderWithNegatives(final DecimalFloat lesser, final DecimalFloat greater)
    {
        assertOrder(lesser, greater);

        final DecimalFloat negativeLesser = lesser.copy().negate();
        final DecimalFloat negativeGreater = greater.copy().negate();

        if (lesser.value() >= 0 && greater.value() >= 0)
        {
            assertOrder(negativeGreater, lesser);
        }

        assertOrder(negativeGreater, negativeLesser);
    }

    private void assertOrder(final DecimalFloat lesser, final DecimalFloat greater)
    {
        assertThat(lesser, lessThan(greater));
        assertThat(greater, greaterThan(lesser));
    }
}
