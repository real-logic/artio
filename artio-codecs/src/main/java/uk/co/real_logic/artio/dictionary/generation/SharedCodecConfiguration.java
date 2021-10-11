/*
 * Copyright 2021 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.dictionary.generation;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class SharedCodecConfiguration
{
    private boolean splitDirectories = true;
    private final List<GeneratorDictionaryConfiguration> dictionaries = new ArrayList<>();

    SharedCodecConfiguration()
    {
    }

    public SharedCodecConfiguration splitDirectories(final boolean splitDirectories)
    {
        this.splitDirectories = splitDirectories;
        return this;
    }

    public SharedCodecConfiguration withDictionary(final String dictionaryName, final String... fileNames)
    {
        if (fileNames.length == 0)
        {
            throw new IllegalArgumentException("Please provide file names for Dictionary");
        }

        return withDictionary(dictionaryName, null, fileNames);
    }

    public SharedCodecConfiguration withDictionary(final String dictionaryName, final InputStream... fileStreams)
    {
        if (fileStreams.length == 0)
        {
            throw new IllegalArgumentException("Please provide file names for Dictionary");
        }

        return withDictionary(dictionaryName, fileStreams, null);
    }

    private SharedCodecConfiguration withDictionary(
        final String dictionaryName, final InputStream[] fileStreams, final String[] fileNames)
    {
        Objects.requireNonNull(dictionaryName);

        dictionaries.add(new GeneratorDictionaryConfiguration(dictionaryName, fileStreams, fileNames));
        return this;
    }

    List<GeneratorDictionaryConfiguration> dictionaries()
    {
        return dictionaries;
    }

    boolean splitDirectories()
    {
        return splitDirectories;
    }
}
