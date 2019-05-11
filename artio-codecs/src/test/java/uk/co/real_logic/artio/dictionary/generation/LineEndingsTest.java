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
package uk.co.real_logic.artio.dictionary.generation;

import org.agrona.generation.StringWriterOutputManager;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

import java.util.Map;

import static uk.co.real_logic.artio.dictionary.ExampleDictionary.*;

public class LineEndingsTest
{
    private static StringWriterOutputManager outputManager = new StringWriterOutputManager();

    @Test
    public void constantGenerator()
    {
        final ConstantGenerator constantGenerator =
            new ConstantGenerator(MESSAGE_EXAMPLE, TEST_PACKAGE, outputManager);
        constantGenerator.generate();
        final Map<String, CharSequence> sources = outputManager.getSources();
        sources.keySet().stream().forEach((key) ->
        {
            final String strippedSource = sources.get(key).toString().replace(System.lineSeparator(), "");
            assertFalse(key + " contains hard-coded line-ending", strippedSource.contains("\n"));
        });
    }

    @Test
    public void enumGenerator()
    {
        final EnumGenerator enumGenerator =
            new EnumGenerator(MESSAGE_EXAMPLE, TEST_PACKAGE, outputManager);
        enumGenerator.generate();
        final Map<String, CharSequence> sources = outputManager.getSources();
        sources.keySet().stream().forEach((key) ->
        {
            final String strippedSource = sources.get(key).toString().replace(System.lineSeparator(), "");
            assertFalse(key + " contains hard-coded line-ending", strippedSource.contains("\n"));
        });
    }

    @Test
    public void encoderGenerator()
    {
        final EncoderGenerator encoderGenerator =
            new EncoderGenerator(MESSAGE_EXAMPLE, 1, TEST_PACKAGE, TEST_PARENT_PACKAGE, outputManager,
            ValidationOn.class, RejectUnknownFieldOff.class);
        encoderGenerator.generate();
        final Map<String, CharSequence> sources = outputManager.getSources();
        sources.keySet().stream().forEach((key) ->
        {
            final String strippedSource = sources.get(key).toString().replace(System.lineSeparator(), "");
            assertFalse(key + " contains hard-coded line-ending", strippedSource.contains("\n"));
        });
    }

    @Test
    public void decoderGenerator()
    {
        final DecoderGenerator decoderGenerator =
            new DecoderGenerator(MESSAGE_EXAMPLE, 1, TEST_PACKAGE, TEST_PARENT_PACKAGE,
            outputManager, ValidationOn.class, RejectUnknownFieldOff.class);
        decoderGenerator.generate();
        final Map<String, CharSequence> sources = outputManager.getSources();
        sources.keySet().stream().forEach((key) ->
        {
            final String strippedSource = sources.get(key).toString().replace(System.lineSeparator(), "");
            assertFalse(key + " contains hard-coded line-ending", strippedSource.contains("\n"));
        });
    }

    @Test
    public void acceptorGenerator()
    {
        final AcceptorGenerator acceptorGenerator =
            new AcceptorGenerator(MESSAGE_EXAMPLE, TEST_PACKAGE, outputManager);
        acceptorGenerator.generate();
        final Map<String, CharSequence> sources = outputManager.getSources();
        sources.keySet().stream().forEach((key) ->
        {
            final String strippedSource = sources.get(key).toString().replace(System.lineSeparator(), "");
            assertFalse(key + " contains hard-coded line-ending", strippedSource.contains("\n"));
        });
    }
}
