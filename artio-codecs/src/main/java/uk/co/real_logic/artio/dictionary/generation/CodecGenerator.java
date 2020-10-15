/*
 * Copyright 2020 Adaptive Financial Consulting Ltd.
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

import org.agrona.generation.PackageOutputManager;
import uk.co.real_logic.artio.builder.RejectUnknownEnumValue;
import uk.co.real_logic.artio.builder.RejectUnknownField;
import uk.co.real_logic.artio.builder.Validation;
import uk.co.real_logic.artio.dictionary.DictionaryParser;
import uk.co.real_logic.artio.dictionary.ir.Dictionary;

import java.io.InputStream;

public final class CodecGenerator
{
    public static void generate(final CodecConfiguration configuration) throws Exception
    {
        configuration.conclude();

        final String outputPath = configuration.outputPath();
        final boolean allowDuplicates = configuration.allowDuplicateFields();
        final DictionaryParser parser = new DictionaryParser(allowDuplicates);
        final String codecRejectUnknownEnumValueEnabled = configuration.codecRejectUnknownEnumValueEnabled();

        Dictionary dictionary = null;
        for (final InputStream fileStream : configuration.fileStreams())
        {
            try
            {
                dictionary = parser.parse(fileStream, dictionary);
            }
            finally
            {
                fileStream.close();
            }
        }

        final String parentPackage = configuration.parentPackage();
        final String encoderPackage = parentPackage + ".builder";
        final String decoderPackage = parentPackage + ".decoder";
        final String decoderFlyweightPackage = parentPackage + ".decoder_flyweight";

        final PackageOutputManager parentOutput = new PackageOutputManager(outputPath, parentPackage);
        final PackageOutputManager decoderOutput = new PackageOutputManager(outputPath, decoderPackage);
        final PackageOutputManager encoderOutput = new PackageOutputManager(outputPath, encoderPackage);

        new EnumGenerator(dictionary, parentPackage, parentOutput).generate();
        new ConstantGenerator(dictionary, parentPackage, parentOutput).generate();

        new FixDictionaryGenerator(
            dictionary,
            parentOutput,
            encoderPackage,
            decoderPackage,
            parentPackage).generate();

        new EncoderGenerator(
            dictionary,
            encoderPackage,
            parentPackage,
            encoderOutput,
            Validation.class,
            RejectUnknownField.class,
            RejectUnknownEnumValue.class,
            codecRejectUnknownEnumValueEnabled).generate();

        new DecoderGenerator(
            dictionary,
            1,
            decoderPackage,
            parentPackage,
            encoderPackage,
            decoderOutput,
            Validation.class,
            RejectUnknownField.class,
            RejectUnknownEnumValue.class,
            false,
            codecRejectUnknownEnumValueEnabled).generate();

        new PrinterGenerator(dictionary, decoderPackage, decoderOutput).generate();
        new AcceptorGenerator(dictionary, decoderPackage, decoderOutput).generate();

        if (configuration.flyweightsEnabled())
        {
            final PackageOutputManager flyweightDecoderOutput =
                new PackageOutputManager(outputPath, decoderFlyweightPackage);

            new DecoderGenerator(
                dictionary,
                1,
                decoderFlyweightPackage,
                parentPackage,
                encoderPackage, flyweightDecoderOutput,
                Validation.class,
                RejectUnknownField.class,
                RejectUnknownEnumValue.class,
                true,
                codecRejectUnknownEnumValueEnabled).generate();
        }
    }
}
