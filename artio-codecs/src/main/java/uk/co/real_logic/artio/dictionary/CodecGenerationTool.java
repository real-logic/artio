/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.dictionary;

import org.agrona.generation.PackageOutputManager;
import uk.co.real_logic.artio.builder.Validation;
import uk.co.real_logic.artio.dictionary.generation.*;
import uk.co.real_logic.artio.dictionary.ir.Dictionary;

import java.io.File;
import java.io.FileInputStream;

import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.*;

public final class CodecGenerationTool
{
    public static void main(final String[] args) throws Exception
    {
        if (args.length < 2)
        {
            printUsageAndExit();
        }

        final String outputPath = args[0];
        final String files = args[1];
        final  String[] fileNames = files.split(";");
        if (fileNames.length > 2)
        {
            System.err.printf("Two many dictionary files(1 or 2 dictionaries supported).%s", files);
            printUsageAndExit();
        }
        Dictionary dictionary = null;
        for (final String fileName : fileNames)
        {
            final File xmlFile = new File(fileName);
            dictionary = parseDictionary(xmlFile, dictionary);
        }

        final PackageOutputManager parent = new PackageOutputManager(outputPath, PARENT_PACKAGE);
        final PackageOutputManager decoder = new PackageOutputManager(outputPath, DECODER_PACKAGE);

        final EnumGenerator enumGenerator = new EnumGenerator(dictionary, PARENT_PACKAGE, parent);
        final ConstantGenerator constantGenerator = new ConstantGenerator(dictionary, PARENT_PACKAGE, parent);

        final EncoderGenerator encoderGenerator = new EncoderGenerator(
            dictionary,
            1,
            ENCODER_PACKAGE,
            PARENT_PACKAGE,
            new PackageOutputManager(outputPath, ENCODER_PACKAGE), Validation.class);

        final DecoderGenerator decoderGenerator = new DecoderGenerator(
            dictionary, 1, DECODER_PACKAGE, PARENT_PACKAGE, decoder, Validation.class);
        final PrinterGenerator printerGenerator = new PrinterGenerator(dictionary, DECODER_PACKAGE, decoder);
        final AcceptorGenerator acceptorGenerator = new AcceptorGenerator(dictionary, DECODER_PACKAGE, decoder);

        enumGenerator.generate();
        constantGenerator.generate();

        encoderGenerator.generate();

        decoderGenerator.generate();
        printerGenerator.generate();
        acceptorGenerator.generate();
    }

    private static Dictionary parseDictionary(final File xmlFile, final Dictionary parentDictionary) throws Exception
    {
        final DictionaryParser parser = new DictionaryParser();
        if (!xmlFile.exists())
        {
            System.err.printf("xmlFile does not exist: %s\n", xmlFile.getAbsolutePath());
            printUsageAndExit();
        }

        if (!xmlFile.isFile())
        {
            System.out.printf("xmlFile isn't a file, are the arguments the correct way around?\n");
            printUsageAndExit();
        }
        try (FileInputStream input = new FileInputStream(xmlFile))
        {
            return parser.parse(input, parentDictionary);
        }
    }

    private static void printUsageAndExit()
    {
        System.err.println("Usage: CodecGenerationTool </path/to/output-directory> " +
            "<[/path/to/fixt-xml/dictionary;]/path/to/xml/dictionary>");
        System.exit(-1);
    }
}
