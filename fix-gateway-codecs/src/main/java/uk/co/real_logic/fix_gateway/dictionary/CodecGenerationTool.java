/*
 * Copyright 2015-2016 Real Logic Ltd.
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

import org.agrona.generation.PackageOutputManager;
import uk.co.real_logic.fix_gateway.builder.Validation;
import uk.co.real_logic.fix_gateway.dictionary.generation.*;
import uk.co.real_logic.fix_gateway.dictionary.ir.Dictionary;

import java.io.FileInputStream;

import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.*;

public final class CodecGenerationTool
{
    public static void main(final String[] args) throws Exception
    {
        final String outputPath = args[0];
        final String xmlPath = args[1];

        if (args.length < 2)
        {
            System.err.println("Usage: CodecGenerationTool </path/to/output-directory> </path/to/xml/dictionary>");
            System.exit(-1);
        }

        final DictionaryParser parser = new DictionaryParser();
        try (FileInputStream input = new FileInputStream(xmlPath))
        {
            final Dictionary dictionary = parser.parse(input);

            final PackageOutputManager parent = new PackageOutputManager(outputPath, PARENT_PACKAGE);
            final PackageOutputManager decoder = new PackageOutputManager(outputPath, DECODER_PACKAGE);

            final EnumGenerator enumGenerator = new EnumGenerator(dictionary, parent);
            final ConstantGenerator constantGenerator = new ConstantGenerator(dictionary, DECODER_PACKAGE, decoder);

            final EncoderGenerator encoderGenerator = new EncoderGenerator(
                dictionary, 1, ENCODER_PACKAGE, new PackageOutputManager(outputPath, ENCODER_PACKAGE), Validation.class);

            final DecoderGenerator decoderGenerator = new DecoderGenerator(
                dictionary, 1, DECODER_PACKAGE, decoder, Validation.class);
            final PrinterGenerator printerGenerator = new PrinterGenerator(dictionary, DECODER_PACKAGE, decoder);
            final AcceptorGenerator acceptorGenerator = new AcceptorGenerator(dictionary, DECODER_PACKAGE, decoder);

            enumGenerator.generate();
            constantGenerator.generate();

            encoderGenerator.generate();

            decoderGenerator.generate();
            printerGenerator.generate();
            acceptorGenerator.generate();
        }
    }
}
