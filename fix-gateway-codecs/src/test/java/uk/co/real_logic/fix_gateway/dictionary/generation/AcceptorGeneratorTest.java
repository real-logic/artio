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
package uk.co.real_logic.fix_gateway.dictionary.generation;

import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.generation.StringWriterOutputManager;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.lang.reflect.Proxy;
import java.util.Map;

import static java.lang.reflect.Modifier.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.agrona.generation.CompilerUtil.compileInMemory;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.*;
import static uk.co.real_logic.fix_gateway.dictionary.generation.AcceptorGenerator.*;

public class AcceptorGeneratorTest
{

    private static StringWriterOutputManager outputManager = new StringWriterOutputManager();
    private static ConstantGenerator constantGenerator =
        new ConstantGenerator(MESSAGE_EXAMPLE, TEST_PACKAGE, outputManager);
    private static DecoderGenerator decoderGenerator = new DecoderGenerator(MESSAGE_EXAMPLE, 1, TEST_PACKAGE, outputManager);
    private static AcceptorGenerator acceptorGenerator = new AcceptorGenerator(MESSAGE_EXAMPLE, TEST_PACKAGE, outputManager);
    private static Class<?> acceptor;
    private static Class<?> decoder;

    private MutableAsciiFlyweight buffer = new MutableAsciiFlyweight(new UnsafeBuffer(new byte[8 * 1024]));

    @BeforeClass
    public static void generate() throws Exception
    {
        constantGenerator.generate();
        decoderGenerator.generate();
        acceptorGenerator.generate();
        final Map<String, CharSequence> sources = outputManager.getSources();
        acceptor = compileInMemory(TEST_PACKAGE + "." + DICTIONARY_ACCEPTOR, sources);
        decoder = acceptor.getClassLoader().loadClass(TEST_PACKAGE + "." + DICTIONARY_DECODER);
        if (acceptor == null || decoder == null)
        {
            System.out.println(sources);
        }
    }

    @Test
    public void shouldGenerateAcceptor()
    {
        assertNotNull("Failed to generate acceptor", acceptor);

        final int modifiers = acceptor.getModifiers();
        assertTrue("Not public", isPublic(modifiers));
        assertTrue("Not interface", isInterface(modifiers));
    }

    @Test
    public void shouldGenerateDecoder()
    {
        assertNotNull("Failed to generate decoder", decoder);

        final int modifiers = decoder.getModifiers();
        assertTrue("Not public", isPublic(modifiers));
        assertFalse("Not instantiable", isAbstract(modifiers));
    }

    @Test
    public void shouldInvokeAppropriateAcceptor() throws Exception
    {
        final boolean[] called = { false };
        final Object acceptorInst = Proxy.newProxyInstance(
            acceptor.getClassLoader(),
            new Class<?>[]{AcceptorGeneratorTest.acceptor},
            (proxy, method, args) ->
            {
                called[0] = true;
                assertEquals("onHeartbeat", method.getName());

                final Object argument = args[0];
                assertNotNull("Missing first argument", argument);
                assertEquals("HeartbeatDecoder", argument.getClass().getSimpleName());

                return null;
            });


        final Object decoderInst = decoder.getDeclaredConstructor(acceptor).newInstance(acceptorInst);

        onMessage(decoderInst);

        assertTrue("Proxy not invoked", called[0]);
    }

    private void onMessage(final Object inst) throws Exception
    {
        buffer.putAscii(1, ENCODED_MESSAGE);
        decoder.getMethod(ON_MESSAGE, AsciiFlyweight.class, int.class, int.class, int.class)
               .invoke(inst, buffer, 1, ENCODED_MESSAGE.length(), '0');
    }

}
