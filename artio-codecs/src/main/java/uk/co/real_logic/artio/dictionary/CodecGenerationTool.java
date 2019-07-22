/*
 * Copyright 2015-2017 Real Logic Ltd.
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

import org.agrona.generation.PackageOutputManager;


import uk.co.real_logic.artio.builder.RejectUnknownEnumValue;
import uk.co.real_logic.artio.builder.RejectUnknownField;
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
        final String[] fileNames = files.split(";");
        if (fileNames.length > 2)
        {
            System.err.print("Two many dictionary files(1 or 2 dictionaries supported)." + files);
            printUsageAndExit();
        }
        Dictionary dictionary = null;
        for (final String fileName : fileNames)
        {
            final File xmlFile = new File(fileName);
            dictionary = parseDictionary(xmlFile, dictionary);
        }

        final PackageOutputManager parentOutput = new PackageOutputManager(outputPath, PARENT_PACKAGE);
        final PackageOutputManager decoderOutput = new PackageOutputManager(outputPath, DECODER_PACKAGE);
        final PackageOutputManager encoderOutput = new PackageOutputManager(outputPath, ENCODER_PACKAGE);

        final EnumGenerator enumGenerator = new EnumGenerator(dictionary, PARENT_PACKAGE, parentOutput);
        final ConstantGenerator constantGenerator = new ConstantGenerator(dictionary, PARENT_PACKAGE, parentOutput);


        final EncoderGenerator encoderGenerator = new EncoderGenerator(
            dictionary,
            ENCODER_PACKAGE,
            PARENT_PACKAGE,
            encoderOutput,
            Validation.class,
            RejectUnknownField.class,
            RejectUnknownEnumValue.class);

        final DecoderGenerator decoderGenerator = new DecoderGenerator(
            dictionary,
            1,
            DECODER_PACKAGE,
            PARENT_PACKAGE,
            decoderOutput,
            Validation.class,
            RejectUnknownField.class,
            RejectUnknownEnumValue.class,
            false);
        final PrinterGenerator printerGenerator = new PrinterGenerator(dictionary, DECODER_PACKAGE, decoderOutput);
        final AcceptorGenerator acceptorGenerator = new AcceptorGenerator(dictionary, DECODER_PACKAGE, decoderOutput);

        enumGenerator.generate();
        constantGenerator.generate();

        encoderGenerator.generate();

        decoderGenerator.generate();
        printerGenerator.generate();
        acceptorGenerator.generate();

        if (FLYWEIGHTS_ENABLED)
        {
            final PackageOutputManager flyweightDecoderOutput =
                new PackageOutputManager(outputPath, DECODER_FLYWEIGHT_PACKAGE);

            final DecoderGenerator flyweightDecoderGenerator = new DecoderGenerator(
                dictionary,
                1,
                DECODER_FLYWEIGHT_PACKAGE,
                PARENT_PACKAGE,
                flyweightDecoderOutput,
                Validation.class,
                RejectUnknownField.class,
                RejectUnknownEnumValue.class,
                true);

            flyweightDecoderGenerator.generate();
        }
    }

    private static Dictionary parseDictionary(final File xmlFile, final Dictionary parentDictionary) throws Exception
    {
        final DictionaryParser parser = new DictionaryParser();
        if (!xmlFile.exists())
        {
            System.err.println("xmlFile does not exist: " + xmlFile.getAbsolutePath());
            printUsageAndExit();
        }

        if (!xmlFile.isFile())
        {
            System.out.println("xmlFile isn't a file, are the arguments the correct way around?");
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
