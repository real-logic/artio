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
package uk.co.real_logic.fix_gateway.fields;

import org.junit.Test;

import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

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
    }

    @Test
    public void compareToOrdersIntegers()
    {
        assertThat(ZERO, lessThan(FIVE));
        assertThat(FIVE, greaterThan(ZERO));

        assertThat(MINUS_FIVE, lessThan(ZERO));
        assertThat(ZERO, greaterThan(MINUS_FIVE));

        assertThat(MINUS_FIVE, lessThan(FIVE));
        assertThat(FIVE, greaterThan(MINUS_FIVE));
    }

    @Test
    public void compareToOrdersFloatsOfSameScale()
    {
        assertThat(POINT_ONE, lessThan(FIVE_POINT_FIVE));
        assertThat(FIVE_POINT_FIVE, greaterThan(POINT_ONE));

        assertThat(MINUS_FIVE_POINT_FIVE, lessThan(POINT_ONE));
        assertThat(POINT_ONE, greaterThan(MINUS_FIVE_POINT_FIVE));

        assertThat(MINUS_FIVE_POINT_FIVE, lessThan(FIVE));
        assertThat(FIVE_POINT_FIVE, greaterThan(MINUS_FIVE_POINT_FIVE));
    }

    @Test
    public void compareToOrdersFloatsWithIntegers()
    {
        assertThat(ZERO, lessThan(POINT_ONE));
        assertThat(POINT_ONE, greaterThan(ZERO));

        assertThat(MINUS_FIVE_POINT_FIVE, lessThan(ZERO));
        assertThat(ZERO, greaterThan(MINUS_FIVE_POINT_FIVE));

        assertThat(MINUS_FIVE_POINT_FIVE, lessThan(FIVE));
        assertThat(FIVE, greaterThan(MINUS_FIVE_POINT_FIVE));
    }

    @Test
    public void compareToOrdersFloatsOfDifferentScale()
    {
        assertThat(new DecimalFloat(45, 1), lessThan(new DecimalFloat(45, 2)));
        assertThat(new DecimalFloat(45, 2), greaterThan(new DecimalFloat(45, 1)));

        assertThat(new DecimalFloat(-45, 2), lessThan(new DecimalFloat(-45, 1)));
        assertThat(new DecimalFloat(-45, 1), greaterThan(new DecimalFloat(-45, 2)));

        assertThat(new DecimalFloat(45, 2), greaterThan(new DecimalFloat(-45, 1)));
        assertThat(new DecimalFloat(-45, 1), lessThan(new DecimalFloat(45, 2)));

        assertThat(ZERO, greaterThan(new DecimalFloat(-45, 1)));
        assertThat(new DecimalFloat(-45, 1), lessThan(ZERO));

        assertThat(new DecimalFloat(45, 2), greaterThan(ZERO));
        assertThat(ZERO, lessThan(new DecimalFloat(45, 2)));
    }
}
