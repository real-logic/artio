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
package uk.co.real_logic.fix_gateway;

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.engine.framer.FakeMilliClock;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class ErrorBufferTest
{
    private UnsafeBuffer unsafeBuffer = new UnsafeBuffer(new byte[64 * 1024]);
    private AtomicCounter mockCounter = mock(AtomicCounter.class);
    private FakeMilliClock clock = new FakeMilliClock();
    private ErrorBuffer writeBuffer = new ErrorBuffer(unsafeBuffer, mockCounter, clock);
    private ErrorBuffer readBuffer = new ErrorBuffer(unsafeBuffer);

    @Before
    public void setUp()
    {
        clock.advanceMilliSeconds(50L);
    }

    @Test
    public void shouldEncodeErrors()
    {
        writeBuffer.onError(ExceptionFixtures.fooException);

        assertOnlyFooException();
    }

    @Test
    public void shouldOnlyStoreLastError()
    {
        writeBuffer.onError(ExceptionFixtures.fooException);
        writeBuffer.onError(ExceptionFixtures.fooException);

        assertOnlyFooException();
    }

    @Test
    public void shouldAggregateErrorsByThrowsite()
    {
        writeBuffer.onError(ExceptionFixtures.fooException);
        writeBuffer.onError(ExceptionFixtures.longerFooException);

        final List<String> errors = readBuffer.errors();
        assertThat(errors, hasSize(1));
        assertIsLongerFooException(errors.get(0));
    }

    @Test
    public void shouldStoreErrorsForDifferentThrowSites()
    {
        writeBuffer.onError(ExceptionFixtures.fooException);
        writeBuffer.onError(ExceptionFixtures.nullPointerException);

        assertFooAndNullPointer();
    }

    @Test
    public void shouldNotOverwriteDifferentException()
    {
        writeBuffer.onError(ExceptionFixtures.fooException);
        writeBuffer.onError(ExceptionFixtures.nullPointerException);

        assertFooAndNullPointer();
    }

    @Test
    public void shouldReplaceShorterExceptionWithoutOverwriting()
    {
        writeBuffer.onError(ExceptionFixtures.fooException);
        writeBuffer.onError(ExceptionFixtures.nullPointerException);
        writeBuffer.onError(ExceptionFixtures.longerFooException);

        final List<String> errors = readBuffer.errors();
        assertThat(errors, hasSize(2));
        assertIsNullPointerException(errors.get(0));
        assertIsLongerFooException(errors.get(1));
    }

    @Test
    public void shouldReplaceLongerExceptionWithoutOverwriting()
    {
        writeBuffer.onError(ExceptionFixtures.longerFooException);
        writeBuffer.onError(ExceptionFixtures.nullPointerException);
        writeBuffer.onError(ExceptionFixtures.fooException);

        assertFooAndNullPointer();
    }

    @Test
    public void shouldCountExceptions()
    {
        writeBuffer.onError(ExceptionFixtures.fooException);
        writeBuffer.onError(ExceptionFixtures.fooException);
        writeBuffer.onError(ExceptionFixtures.nullPointerException);

        verify(mockCounter, times(3)).orderedIncrement();
    }


    @Test
    public void shouldNotDisplayExceptionsBeforeTimestamp()
    {
        writeBuffer.onError(ExceptionFixtures.fooException);

        assertThat(readBuffer.errorsSince(100L), hasSize(0));
    }

    @Test
    public void shouldDisplayExceptionsSinceTimestamp()
    {
        clock.advanceMilliSeconds(150L);

        writeBuffer.onError(ExceptionFixtures.fooException);

        final List<String> errors = readBuffer.errorsSince(100L);
        assertThat(errors, hasSize(1));
        assertIsFooException(errors.get(0));
    }

    private void assertFooAndNullPointer()
    {
        final List<String> errors = readBuffer.errors();
        assertThat(errors, hasSize(2));
        assertIsFooException(errors.get(0));
        assertIsNullPointerException(errors.get(1));
    }

    private void assertOnlyFooException()
    {
        final List<String> errors = readBuffer.errors();
        assertThat(errors, hasSize(1));
        assertIsFooException(errors.get(0));
    }

    private void assertIsFooException(final String error)
    {
        assertStartOfFooException(error);
        assertThat(error, not(containsExtraMethod()));
    }

    private void assertIsLongerFooException(final String error)
    {
        assertStartOfFooException(error);
        assertThat(error, containsExtraMethod());
    }

    private void assertStartOfFooException(final String error)
    {
        assertThat(error,
            containsString("uk.co.real_logic.fix_gateway.ExceptionFixtures.foo(ExceptionFixtures.java:71)"));
        assertThat(error,
            containsString("java.lang.RuntimeException"));
    }

    private Matcher<String> containsExtraMethod()
    {
        return containsString(
            "uk.co.real_logic.fix_gateway.ExceptionFixtures.someLikeRealllllllyLongMethodName" +
                "(ExceptionFixtures.java:56)");
    }

    private void assertIsNullPointerException(final String error)
    {
        assertThat(error,
            containsString("uk.co.real_logic.fix_gateway.ExceptionFixtures.baz(ExceptionFixtures.java:61)"));
        assertThat(error,
            containsString("java.lang.NullPointerException"));
        assertThat(error,
            containsString("uk.co.real_logic.fix_gateway.ExceptionFixtures.<clinit>(ExceptionFixtures.java:46)"));
    }

}
