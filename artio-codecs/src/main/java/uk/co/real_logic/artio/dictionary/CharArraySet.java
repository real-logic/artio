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
package uk.co.real_logic.artio.dictionary;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public final class CharArraySet
{
    private final CharArrayWrapper wrapper = new CharArrayWrapper();
    private final Set<CharArrayWrapper> values;

    public CharArraySet(final String... values)
    {
        this(Arrays.asList(values));
    }

    public CharArraySet(final Collection<String> values)
    {
        this.values = values
            .stream()
            .map(CharArrayWrapper::new)
            .collect(toSet());
    }

    public CharArraySet(final CharArraySet other)
    {
        this.values = other.values
            .stream()
            .map(CharArrayWrapper::new)
            .collect(toSet());
    }

    public boolean contains(final char[] value, final int length)
    {
        wrapper.wrap(value, length);
        return values.contains(wrapper);
    }
}
