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
package uk.co.real_logic.artio.dictionary;

import uk.co.real_logic.artio.dictionary.ir.Dictionary;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

public class CodecCollisionFinder
{
    public static void main(final String[] args) throws Exception
    {
        final File dir = new File(args[0]);
        final File[] dictionaryFiles = dir.listFiles((ignore, name) -> name.endsWith(".xml"));
        final DictionaryParser parser = new DictionaryParser(true);
        final List<Dictionary> dictionaries = new ArrayList<>();
        for (final File dictionaryXmlFile : dictionaryFiles)
        {
            System.out.println("Parsing " + dictionaryXmlFile.getName());
            try (FileInputStream in = new FileInputStream(dictionaryXmlFile))
            {
                final Dictionary dictionary = parser.parse(in, null);
                dictionaries.add(dictionary);
            }
        }

        System.out.println(dictionaries.size());
    }
}
