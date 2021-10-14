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

import uk.co.real_logic.artio.dictionary.generation.CodecConfiguration;
import uk.co.real_logic.artio.dictionary.generation.CodecGenerator;
import uk.co.real_logic.artio.dictionary.generation.SharedCodecConfiguration;

import java.io.File;
import java.util.Arrays;

public final class CodecSharingExample
{
    public static void main(final String[] args) throws Exception
    {
        final String dictionaryDirectory = args[0];
        final String outputPath = args[1];
        final boolean shouldShare = Boolean.parseBoolean(args[2]);

        final File dir = new File(dictionaryDirectory);
        final File[] dictionaryFiles = dir.listFiles((ignore, name) -> name.endsWith(".xml"));

        System.out.println("Found Input Dictionaries: " + Arrays.toString(dictionaryFiles));

        final String[] dictNames = Arrays.stream(dictionaryFiles)
            .map(file ->
            {
                final String name = file.getName();
                return name.substring(0, name.length() - 4);
            })
            .toArray(String[]::new);

        if (shouldShare)
        {
            final String[] fileNames = Arrays.stream(dictionaryFiles).map(File::getAbsolutePath).toArray(String[]::new);

            final CodecConfiguration config = new CodecConfiguration().outputPath(outputPath);

            final SharedCodecConfiguration sharedCodecs = config.sharedCodecsEnabled();
            for (int i = 0; i < dictionaryFiles.length; i++)
            {
                final String fileName = new File(fileNames[i]).getName();
                final File dictionaryFile = dictionaryFiles[i];
                sharedCodecs.withDictionary(fileName, dictionaryFile.getAbsolutePath());
            }

            CodecGenerator.generate(config);
        }
        else
        {
            for (int i = 0; i < dictionaryFiles.length; i++)
            {
                System.out.println(dictNames[i]);

                final String dictionaryOutputPath = outputPath + File.separator + dictNames[i];
                final String dictionaryPath = dictionaryFiles[i].getAbsolutePath();

                final CodecConfiguration config = new CodecConfiguration()
                    .outputPath(dictionaryOutputPath)
                    .fileNames(dictionaryPath);

                CodecGenerator.generate(config);
            }
        }
    }
}
