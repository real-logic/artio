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
package uk.co.real_logic.artio.dictionary.generation;

import org.agrona.generation.StringWriterOutputManager;
import uk.co.real_logic.artio.builder.Decoder;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.dictionary.ExampleDictionary;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

class CodecGenerationWrapper
{
    static InputStream dictionaryStream(final String dict)
    {
        return ExampleDictionary.class.getResourceAsStream(dict + ".xml");
    }

    private final Map<String, CharSequence> sources = new HashMap<>();
    private final List<StringWriterOutputManager> outputManagers = new ArrayList<>();

    private CodecConfiguration config;
    private ClassLoader classLoader;

    private MutableAsciiBuffer buffer;
    private int length;
    private int offset;

    void assertEncodes(final Encoder encoder, final String msg)
    {
        encode(encoder);

        final String encoded = buffer.getStringWithoutLengthAscii(offset, length);
        assertEquals(encoder.toString(), msg, encoded);
    }

    void encode(final Encoder encoder)
    {
        newBuffer();
        final long result = encoder.encode(buffer, 0);
        length = Encoder.length(result);
        offset = Encoder.offset(result);
    }

    void newBuffer()
    {
        buffer = new MutableAsciiBuffer(new byte[1024]);
    }

    void generate(final Consumer<CodecConfiguration> configurer) throws Exception
    {
        config = new CodecConfiguration()
            .outputPath("ignored")
            .outputManagerFactory((outputPath, parentPackage) ->
            {
                final StringWriterOutputManager outputManager = new StringWriterOutputManager();
                outputManager.setPackageName(parentPackage);
                outputManagers.add(outputManager);
                return outputManager;
            });

        configurer.accept(config);

        CodecGenerator.generate(config);

        for (final StringWriterOutputManager outputManager : outputManagers)
        {
            sources.putAll(outputManager.getSources());
        }

        // System.out.println(SOURCES.values().stream().mapToInt(CharSequence::length).sum());

        if (AbstractDecoderGeneratorTest.CODEC_LOGGING)
        {
            System.out.println(sources);
        }
    }

    Class<?> compile(final String className) throws ClassNotFoundException
    {
        final Class<?> cls = GenerationCompileUtil.compileCleanInMemory(className, sources);
        classLoader = cls.getClassLoader();
        return cls;
    }

    CodecConfiguration config()
    {
        return config;
    }

    void noClass(final String name)
    {
        try
        {
            loadClass(name);
            fail("Managed to load " + name + " which shouldn't exist");
        }
        catch (final ClassNotFoundException e)
        {
            // Deliberately blank
        }
    }

    @SuppressWarnings("unchecked")
    <T> Class<T> loadClass(final String name) throws ClassNotFoundException
    {
        try
        {
            return (Class<T>)classLoader.loadClass(name);
        }
        catch (final NullPointerException e)
        {
            throw new ClassNotFoundException("Class not found: " + name, e);
        }
    }

    Map<String, CharSequence> sources()
    {
        return sources;
    }

    String encoder(final String dictNorm, final String messageName)
    {
        return className(dictNorm, messageName, "Encoder", "builder.");
    }

    Class<?> encoder(final String messageName) throws ClassNotFoundException
    {
        return loadClass(encoder(null, messageName));
    }

    String decoder(final String dictNorm, final String messageName)
    {
        return className(dictNorm, messageName, "Decoder", "decoder.");
    }

    Class<?> decoder(final String messageName) throws ClassNotFoundException
    {
        return loadClass(decoder(null, messageName));
    }

    String className(
        final String dictNorm,
        final String messageName,
        final String suffix,
        final String prefix)
    {
        final String packagePrefix = dictNorm == null ? "" : "." + dictNorm;
        return config.parentPackage() + packagePrefix + "." + prefix + messageName + suffix;
    }

    public void decode(final Decoder decoder)
    {
        decoder.decode(buffer, offset, length);
    }

    public void decode(final Decoder decoder, final String message)
    {
        newBuffer();
        final int length = buffer.putStringWithoutLengthAscii(0, message);
        decoder.decode(buffer, 0, length);
    }

    static void setupHeader(final Encoder encoder)
    {
        encoder
            .header()
            .senderCompID("sender")
            .targetCompID("target")
            .msgSeqNum(1)
            .sendingTime(" ".getBytes(StandardCharsets.US_ASCII));
    }
}
