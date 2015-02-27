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

package uk.co.real_logic.fix_gateway.util;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import uk.co.real_logic.agrona.DirectBuffer;

import java.util.Objects;

/**
 * Custom hamcrest matchers to support our own types in tests.
 */
public final class CustomMatchers
{

    private CustomMatchers()
    {
    }

    /**
     * Assert that a range of an ascii flyweight equals a String.
     */
    public static Matcher<AsciiFlyweight> asciiString(final String expectedValue, final int offset, final int length)
    {
        Objects.requireNonNull(expectedValue);

        return new TypeSafeMatcher<AsciiFlyweight>()
        {
            private String string;

            protected boolean matchesSafely(final AsciiFlyweight item)
            {
                this.string = item.getRangeAsString(offset, length);

                return expectedValue.equals(string);
            }

            public void describeTo(final Description description)
            {
                description.appendText("Expected a String like: ");
                description.appendValue(expectedValue);
                description.appendText("But actually was: ");
                description.appendValue(string);
            }
        };
    }

    public static Matcher<DirectBuffer> containsString(final String expectedValue, final int offset, final int length)
    {
        Objects.requireNonNull(expectedValue);

        return new TypeSafeMatcher<DirectBuffer>()
        {
            private final Matcher<AsciiFlyweight> flyweightMatcher = asciiString(expectedValue, offset, length);

            protected boolean matchesSafely(final DirectBuffer item)
            {
                return flyweightMatcher.matches(new AsciiFlyweight(item));
            }

            public void describeTo(final Description description)
            {
                flyweightMatcher.describeTo(description);
            }
        };
    }
}
