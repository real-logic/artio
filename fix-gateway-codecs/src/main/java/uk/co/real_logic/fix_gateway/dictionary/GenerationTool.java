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

import uk.co.real_logic.agrona.generation.PackageOutputManager;
import uk.co.real_logic.fix_gateway.dictionary.generation.DecoderGenerator;
import uk.co.real_logic.fix_gateway.dictionary.generation.EncoderGenerator;
import uk.co.real_logic.fix_gateway.dictionary.generation.EnumGenerator;
import uk.co.real_logic.fix_gateway.dictionary.generation.PrinterGenerator;
import uk.co.real_logic.fix_gateway.dictionary.ir.DataDictionary;

import java.io.FileInputStream;

import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.DECODER_PACKAGE;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.ENCODER_PACKAGE;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.PARENT_PACKAGE;

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

            final EnumGenerator enumGenerator = new EnumGenerator(dictionary,
                new PackageOutputManager(outputPath, PARENT_PACKAGE));

            final EncoderGenerator encoderGenerator = new EncoderGenerator(dictionary, 20, ENCODER_PACKAGE,
                new PackageOutputManager(outputPath, ENCODER_PACKAGE));

            final DecoderGenerator decoderGenerator = new DecoderGenerator(dictionary, 20, DECODER_PACKAGE,
                    new PackageOutputManager(outputPath, DECODER_PACKAGE));

            final PrinterGenerator printerGenerator = new PrinterGenerator(dictionary, DECODER_PACKAGE,
                new PackageOutputManager(outputPath, DECODER_PACKAGE));

            enumGenerator.generate();
            encoderGenerator.generate();
            decoderGenerator.generate();
            printerGenerator.generate();
        }
    }
}
