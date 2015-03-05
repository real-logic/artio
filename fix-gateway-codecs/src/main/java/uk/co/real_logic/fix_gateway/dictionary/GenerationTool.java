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
package uk.co.real_logic.fix_gateway.dictionary;

import uk.co.real_logic.agrona.generation.OutputManager;
import uk.co.real_logic.agrona.generation.PackageOutputManager;
import uk.co.real_logic.fix_gateway.dictionary.generation.EncoderGenerator;
import uk.co.real_logic.fix_gateway.dictionary.generation.EnumGenerator;
import uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil;
import uk.co.real_logic.fix_gateway.dictionary.ir.DataDictionary;

import java.io.FileInputStream;

public final class GenerationTool
{
    public static void main(String[] args) throws Exception
    {
        final String outputPath = args[0];
        final String xmlPath = args[1];
        final DictionaryParser parser = new DictionaryParser();
        try (final FileInputStream input = new FileInputStream(xmlPath))
        {
            final DataDictionary dictionary = parser.parse(input);
            final OutputManager outputManager = new PackageOutputManager(outputPath, GenerationUtil.BUILDER_PACKAGE);
            final EnumGenerator enumGenerator = new EnumGenerator(dictionary, outputManager);
            final EncoderGenerator encoderGenerator = new EncoderGenerator(dictionary, 20, outputManager);

            enumGenerator.generate();
            encoderGenerator.generate();
        }
    }
}
