package uk.co.real_logic.artio.fields;

import org.junit.Test;

import java.math.BigDecimal;
import java.util.Locale;

import static java.math.RoundingMode.UNNECESSARY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class DecimalFloatToAndFromDoubleTest
{
    private static final long DECIMAL_MAX_VALUE = 999999999999999999L;
    private static final long DECIMAL_MIN_VALUE = -DECIMAL_MAX_VALUE;

    @Test
    public void shouldExtractNaNFromDecimalNumber()
    {
        //Given
        final DecimalFloat decimalFloat = new DecimalFloat();
        decimalFloat.fromDouble(Double.NaN);

        //When
        final double value = decimalFloat.toDouble();

        //Then
        assertThat(value, is(Double.NaN));
    }

    @Test
    public void shouldConvertFromDoubleAccurately()
    {
        assertEquals(String.format(Locale.US, "%.0f", 0.0),
            buildDecimalFloatStringFromDouble(0.0));
        assertEquals(String.format(Locale.US, "%.0f", 1.0),
            buildDecimalFloatStringFromDouble(1.0));
        assertEquals(String.format(Locale.US, "%.0f", -1.0),
            buildDecimalFloatStringFromDouble(-1.0));
        assertEquals(String.format(Locale.US, "%.0f", 87653.0),
            buildDecimalFloatStringFromDouble(87653.0));
        assertEquals(String.format(Locale.US, "%.0f", -237849.0),
            buildDecimalFloatStringFromDouble(-237849.0));

        assertEquals(String.format(Locale.US, "%.2f", 0.01),
            buildDecimalFloatStringFromDouble(0.01));
        assertEquals(String.format(Locale.US, "%.1f", 0.1),
            buildDecimalFloatStringFromDouble(0.1));
        assertEquals(String.format(Locale.US, "%.1f", -0.1),
            buildDecimalFloatStringFromDouble(-0.1));
        assertEquals(String.format(Locale.US, "%.5f", 0.98374),
            buildDecimalFloatStringFromDouble(0.98374));
        assertEquals(String.format(Locale.US, "%.6f", 0.983745),
            buildDecimalFloatStringFromDouble(0.983745));
        assertEquals(String.format(Locale.US, "%.6f", -7284.928374),
            buildDecimalFloatStringFromDouble(-7284.928374));
        assertEquals(String.format(Locale.US, "%.8f", 0.00007284),
            buildDecimalFloatStringFromDouble(0.00007284));
        assertEquals(String.format(Locale.US, "%.14f", -0.00000000000001),
            buildDecimalFloatStringFromDouble(-0.00000000000001));
        assertEquals(String.format(Locale.US, "%.4f", 10001.0001),
            buildDecimalFloatStringFromDouble(10001.0001));
        assertEquals(String.format(Locale.US, "%.53f", 1.0e-53),
            buildDecimalFloatStringFromDouble(1.0e-53));
        assertEquals(String.format(Locale.US, "%.53f", -1.0e-53),
            buildDecimalFloatStringFromDouble(-1.0e-53));
    }

    @Test
    public void shouldNotConvertDoublesOutsideOfAllowedLimits()
    {
        assertFalse(new DecimalFloat().fromDouble(Double.MAX_VALUE));
        assertFalse(new DecimalFloat().fromDouble(-Double.MAX_VALUE));

        assertFalse(new DecimalFloat().fromDouble(DECIMAL_MAX_VALUE + 1.0));
        assertFalse(new DecimalFloat().fromDouble(DECIMAL_MIN_VALUE - 1.0));
    }

    @Test
    public void shouldConvertNearZeroDoublesToZero()
    {
        final StringBuilder builder = new StringBuilder();
        assertEquals("0", buildDecimalFloatStringFromDouble(Double.MIN_VALUE));
        assertEquals("0", buildDecimalFloatStringFromDouble(-Double.MIN_VALUE));

        assertEquals("0", buildDecimalFloatStringFromDouble(1.0e-153));
        assertEquals("0", buildDecimalFloatStringFromDouble(-1.0e-153));
    }

    @Test
    public void shouldConvertToDoubleAccuratelyUpTo23OrdersOfMagnitude()
    {
        assertEquals(longBitsFromBigDecimal(0, 0), longBitsFromDecimalFloat(0, 0));
        assertEquals(longBitsFromBigDecimal(1, 0), longBitsFromDecimalFloat(1, 0));
        assertEquals(longBitsFromBigDecimal(5, 1), longBitsFromDecimalFloat(5, 1));
        assertEquals(longBitsFromBigDecimal(-25, 2), longBitsFromDecimalFloat(-25, 2));

        final long[] values = new long[]{
            1L, -2L, 3L, -17L, 53L, -101L, 234L, -9247L, 98237492L, -172839473229L, 329805234980580L,
            DECIMAL_MIN_VALUE, DECIMAL_MAX_VALUE
        };

        for (final long value : values)
        {
            for (int i = 0; i < 23; i++)
            {
                assertEquals(longBitsFromBigDecimal(value, i), longBitsFromDecimalFloat(value, i));
            }
        }
    }

    private static String buildDecimalFloatStringFromDouble(final double number)
    {
        final DecimalFloat decimalFloat = new DecimalFloat();
        if (!decimalFloat.fromDouble(number))
        {
            assertEquals(DecimalFloat.NAN, decimalFloat);
            fail("Invalid input: " + number);
        }
        return decimalFloat.toString();
    }

    private static long longBitsFromDecimalFloat(final long value, final int scale)
    {
        final DecimalFloat decimalFloat = new DecimalFloat(value, scale);
        return Double.doubleToLongBits(decimalFloat.toDouble());
    }

    private static long longBitsFromBigDecimal(final long value, final int scale)
    {
        final BigDecimal divisor = new BigDecimal(10).pow(scale).setScale(scale, UNNECESSARY);
        final BigDecimal bigDecimal = new BigDecimal(value).setScale(scale, UNNECESSARY);
        final BigDecimal divide = bigDecimal.divide(divisor, UNNECESSARY);
        return Double.doubleToLongBits(divide.doubleValue());
    }
}