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

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

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
    public static Matcher<AsciiFlyweight> containsAscii(final String expectedValue, final int offset, final int length)
    {
        Objects.requireNonNull(expectedValue);

        return new TypeSafeMatcher<AsciiFlyweight>()
        {
            private String string;

            protected boolean matchesSafely(final AsciiFlyweight item)
            {
                this.string = item.getAscii(offset, length);

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

    public static Matcher<AsciiFlyweight> startsWithAscii(final String expectedValue)
    {
        return containsAscii(expectedValue, 0, expectedValue.length());
    }

    public static Matcher<DirectBuffer> containsString(final String expectedValue, final int offset, final int length)
    {
        Objects.requireNonNull(expectedValue);

        return new TypeSafeMatcher<DirectBuffer>()
        {
            private final Matcher<AsciiFlyweight> flyweightMatcher = containsAscii(expectedValue, offset, length);

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

    /**
     * Doesn't use getters for properties, like hamcrest.
     */
    public static <T> Matcher<T> hasProperty(final String name, final Matcher<?> valueMatcher)
    {
        return new TypeSafeMatcher<T>()
        {
            public String error = null;

            protected boolean matchesSafely(final T item)
            {
                try
                {
                    final Object value = item.getClass().getMethod(name).invoke(item);
                    return valueMatcher.matches(value);
                }
                catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e)
                {
                    error = e.getMessage();
                    return false;
                }
            }

            public void describeTo(final Description description)
            {
                if (error != null)
                {
                    description.appendText(error);
                }
                else
                {
                    description.appendText("A method called " + name + " with ");
                    valueMatcher.describeTo(description);
                }
            }
        };
    }

    public static void assertCharsEquals(final String expectedValue, final char[] chars, final int length)
    {
        assertEquals("length wasn't equal", expectedValue.length(), length);
        final String value = new String(chars, 0, length);
        assertEquals(expectedValue, value);
    }
}
