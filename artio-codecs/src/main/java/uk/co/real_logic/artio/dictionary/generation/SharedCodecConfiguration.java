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

/**
 * Configuration object for setting shared dictionary generation configuration on.
 */
public final class SharedCodecConfiguration
{
    private final boolean allowDuplicateFieldsDefault;
    private boolean splitDirectories = true;
    private final List<GeneratorDictionaryConfiguration> dictionaries = new ArrayList<>();

    SharedCodecConfiguration(final boolean allowDuplicateFieldsDefault)
    {
        this.allowDuplicateFieldsDefault = allowDuplicateFieldsDefault;
    }

    /**
     * Sets whether to split the directory structure or not. If true then each dictionary's Java source code will be
     * generated into a separate directory, using the name of the dictionary as a suffix to the
     * {@link CodecConfiguration#outputPath(String)}. If false they will all be generated in
     * {@link CodecConfiguration#outputPath(String)}. Defaults to true.
     *
     * @param splitDirectories true to split the structure, false otherwise.
     * @return this
     */
    public SharedCodecConfiguration splitDirectories(final boolean splitDirectories)
    {
        this.splitDirectories = splitDirectories;
        return this;
    }

    /**
     * Add a dictionary, using file names, to the shared dictionaries.
     *
     * @param dictionaryName the name to use for this dictionary.
     * @param fileNames see {@link CodecConfiguration#fileNames(String...)} for details.
     * @return this
     */
    public SharedCodecConfiguration withDictionary(final String dictionaryName, final String... fileNames)
    {
        return withDictionary(dictionaryName, allowDuplicateFieldsDefault, fileNames);
    }

    /**
     * Add a dictionary, using file names, to the shared dictionaries.
     *
     * @param dictionaryName the name to use for this dictionary.
     * @param allowDuplicateFields see {@link CodecConfiguration#allowDuplicateFields(boolean)} for details.
     * @param fileNames see {@link CodecConfiguration#fileNames(String...)} for details.
     * @return this
     */
    public SharedCodecConfiguration withDictionary(
        final String dictionaryName, final boolean allowDuplicateFields, final String... fileNames)
    {
        if (fileNames.length == 0)
        {
            throw new IllegalArgumentException("Please provide file names for Dictionary");
        }

        return withDictionary(dictionaryName, null, fileNames, allowDuplicateFields);
    }

    /**
     * Add a dictionary, using input streams, to the shared dictionaries.
     *
     * @param dictionaryName the name to use for this dictionary.
     * @param fileStreams see {@link CodecConfiguration#fileStreams(InputStream...)} for details.
     * @return this
     */
    public SharedCodecConfiguration withDictionary(final String dictionaryName, final InputStream... fileStreams)
    {
        return withDictionary(dictionaryName, allowDuplicateFieldsDefault, fileStreams);
    }

    /**
     * Add a dictionary, using input streams, to the shared dictionaries.
     *
     * @param dictionaryName the name to use for this dictionary.
     * @param allowDuplicateFields see {@link CodecConfiguration#allowDuplicateFields(boolean)} for details.
     * @param fileStreams see {@link CodecConfiguration#fileStreams(InputStream...)} for details.
     * @return this
     */
    public SharedCodecConfiguration withDictionary(
        final String dictionaryName, final boolean allowDuplicateFields, final InputStream... fileStreams)
    {
        if (fileStreams.length == 0)
        {
            throw new IllegalArgumentException("Please provide file names for Dictionary");
        }

        return withDictionary(dictionaryName, fileStreams, null, allowDuplicateFields);
    }

    private SharedCodecConfiguration withDictionary(
        final String dictionaryName,
        final InputStream[] fileStreams,
        final String[] fileNames,
        final boolean allowDuplicateFields)
    {
        Objects.requireNonNull(dictionaryName);

        dictionaries.add(new GeneratorDictionaryConfiguration(
            dictionaryName, fileStreams, fileNames, allowDuplicateFields));
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
