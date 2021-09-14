/*
 * Copyright 2021 Monotonic Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import java.util.Objects;
import java.util.function.Predicate;

abstract class CompositePredicate<T> implements Predicate<T>
{
    final Predicate<T> left;
    final Predicate<? super T> right;

    CompositePredicate(final Predicate<T> left, final Predicate<? super T> right)
    {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        this.left = left;
        this.right = right;
    }

    Predicate<T> left()
    {
        return left;
    }

    @SuppressWarnings("unchecked")
    Predicate<T> right()
    {
        return (Predicate<T>)right;
    }

    public Predicate<T> and(final Predicate<? super T> other)
    {
        return new And<T>(this, other);
    }

    public Predicate<T> or(final Predicate<? super T> other)
    {
        return new Or<T>(this, other);
    }
}

class And<T> extends CompositePredicate<T>
{
    And(final Predicate<T> left, final Predicate<? super T> right)
    {
        super(left, right);
    }

    public boolean test(final T t)
    {
        return left.test(t) && right.test(t);
    }

    public String toString()
    {
        return "And{" +
            "left=" + left +
            ", right=" + right +
            '}';
    }
}

class Or<T> extends CompositePredicate<T>
{
    Or(final Predicate<T> left, final Predicate<? super T> right)
    {
        super(left, right);
    }

    public boolean test(final T t)
    {
        return left.test(t) || right.test(t);
    }

    public String toString()
    {
        return "Or{" +
            "left=" + left +
            ", right=" + right +
            '}';
    }
}
