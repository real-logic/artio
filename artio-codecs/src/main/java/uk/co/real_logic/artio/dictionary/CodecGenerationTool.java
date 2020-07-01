/*
 * Copyright 2015-2020 Real Logic Limited., Monotonic Ltd.
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
    /**
     * Boolean system property to turn on or off duplicated fields validation. Defaults to false.
     * <p>
     * Fix specification vol 1:
     * A tag number (field) should only appear in a message once. If it appears more than once in the message it should
     * be considered an error with the specification document.
     * <p>
     * Turning this option on may break parsing: this option should be used for support fix specification with error
     * only. It is recommended, where possible, to correct your FIX XML file instead of using this option in order
     * to support an invalid XML file.
     * <br>
     * The duplicated fields is allowed in the following case:
     * <pre>
     * message body:
     * field;
     * repeating group:
     * the_other_field+
     * field;
     * </pre>
     */
    public static final String FIX_CODECS_ALLOW_DUPLICATE_FIELDS = "fix.codecs.allow_duplicate_fields";

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

        new EnumGenerator(dictionary, PARENT_PACKAGE, parentOutput).generate();
        new ConstantGenerator(dictionary, PARENT_PACKAGE, parentOutput).generate();

        new FixDictionaryGenerator(
            dictionary,
            parentOutput,
            ENCODER_PACKAGE,
            DECODER_PACKAGE,
            PARENT_PACKAGE).generate();

        final String codecRejectUnknownEnumValueEnabled = HARD_CODED_REJECT_UNKNOWN_EMUM_VALUES
            .map(String::valueOf)
            .orElse(Generator.RUNTIME_REJECT_UNKNOWN_ENUM_VALUE_PROPERTY);

        new EncoderGenerator(
            dictionary,
            ENCODER_PACKAGE,
            PARENT_PACKAGE,
            encoderOutput,
            Validation.class,
            RejectUnknownField.class,
            RejectUnknownEnumValue.class,
            codecRejectUnknownEnumValueEnabled).generate();

        new DecoderGenerator(
            dictionary,
            1,
            DECODER_PACKAGE,
            PARENT_PACKAGE,
            ENCODER_PACKAGE,
            decoderOutput,
            Validation.class,
            RejectUnknownField.class,
            RejectUnknownEnumValue.class,
            false,
            codecRejectUnknownEnumValueEnabled).generate();

        new PrinterGenerator(dictionary, DECODER_PACKAGE, decoderOutput).generate();
        new AcceptorGenerator(dictionary, DECODER_PACKAGE, decoderOutput).generate();

        if (FLYWEIGHTS_ENABLED)
        {
            final PackageOutputManager flyweightDecoderOutput =
                new PackageOutputManager(outputPath, DECODER_FLYWEIGHT_PACKAGE);

            new DecoderGenerator(
                dictionary,
                1,
                DECODER_FLYWEIGHT_PACKAGE,
                PARENT_PACKAGE,
                ENCODER_PACKAGE, flyweightDecoderOutput,
                Validation.class,
                RejectUnknownField.class,
                RejectUnknownEnumValue.class,
                true,
                codecRejectUnknownEnumValueEnabled).generate();
        }
    }

    private static Dictionary parseDictionary(final File xmlFile, final Dictionary parentDictionary) throws Exception
    {
        final DictionaryParser parser = new DictionaryParser(
            Boolean.getBoolean(CodecGenerationTool.FIX_CODECS_ALLOW_DUPLICATE_FIELDS));
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
