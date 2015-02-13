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
    private DecimalFloat _0 = new DecimalFloat(0, 0);
    private DecimalFloat _5 = new DecimalFloat(5, 0);
    private DecimalFloat minus5 = new DecimalFloat(-5, 0);

    private DecimalFloat point1 = new DecimalFloat(1, 1);
    private DecimalFloat _5point5 = new DecimalFloat(55, 1);
    private DecimalFloat minus5point5 = new DecimalFloat(-55, 1);

    @Test
    public void compareToDetectsEqualIntegers()
    {
        assertThat(_0, comparesEqualTo(_0));
        assertThat(_5, comparesEqualTo(new DecimalFloat(5, 0)));
        assertThat(minus5, comparesEqualTo(new DecimalFloat(-5, 0)));
    }

    @Test
    public void compareToOrdersIntegers()
    {
        assertThat(_0, lessThan(_5));
        assertThat(_5, greaterThan(_0));

        assertThat(minus5, lessThan(_0));
        assertThat(_0, greaterThan(minus5));

        assertThat(minus5, lessThan(_5));
        assertThat(_5, greaterThan(minus5));
    }

    @Test
    public void compareToOrdersFloatsOfSameScale()
    {
        assertThat(point1, lessThan(_5point5));
        assertThat(_5point5, greaterThan(point1));

        assertThat(minus5point5, lessThan(point1));
        assertThat(point1, greaterThan(minus5point5));

        assertThat(minus5point5, lessThan(_5));
        assertThat(_5point5, greaterThan(minus5point5));
    }

    @Test
    public void compareToOrdersFloatsWithIntegers()
    {
        assertThat(_0, lessThan(point1));
        assertThat(point1, greaterThan(_0));

        assertThat(minus5point5, lessThan(_0));
        assertThat(_0, greaterThan(minus5point5));

        assertThat(minus5point5, lessThan(_5));
        assertThat(_5, greaterThan(minus5point5));
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

        assertThat(_0, greaterThan(new DecimalFloat(-45, 1)));
        assertThat(new DecimalFloat(-45, 1), lessThan(_0));

        assertThat(new DecimalFloat(45, 2), greaterThan(_0));
        assertThat(_0, lessThan(new DecimalFloat(45, 2)));
    }
}
