/*
 * Copyright 2013 Real Logic Ltd.
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

import org.agrona.Verify;

import java.util.stream.Stream;

import static java.lang.Character.isUpperCase;
import static java.lang.Character.toUpperCase;
import static java.util.stream.Collectors.joining;

public final class GenerationUtil
{
    public static final int MESSAGE_TYPE_BITSHIFT = 8;
    public static final String PARENT_PACKAGE = System.getProperty("PARENT_PACKAGE", "uk.co.real_logic.artio");

    public static final String ENCODER_PACKAGE = PARENT_PACKAGE + ".builder";
    public static final String DECODER_PACKAGE = PARENT_PACKAGE + ".decoder";
    public static final String INDENT = "    ";

    private GenerationUtil()
    {
    }

    public static String fileHeader(final String packageName)
    {
        return String.format(
            "/* Generated Fix Gateway message codec */\n" +
            "package %s;\n\n",
            packageName);
    }

    public static int packMessageType(final String representation)
    {
        int packed = (byte)representation.charAt(0);

        if (representation.length() == 2)
        {
            final int second = (int)representation.charAt(1);
            packed |= second << MESSAGE_TYPE_BITSHIFT;
        }

        return packed;
    }

    public static String constantName(final String name)
    {
        final String replacedName = name.replace("ID", "Id")
            .replace("GroupCounter", "");
        return toUpperCase(replacedName.charAt(0)) + replacedName
            .substring(1)
            .chars()
            .mapToObj((codePoint) -> (isUpperCase(codePoint) ? "_" : "") + (char)toUpperCase(codePoint))
            .collect(joining());
    }

    public static class Var
    {
        private final String type;
        private final String methodArgsType;
        private final String name;

        public Var(final String type, final String methodArgsType, final String name)
        {
            this.type = type;
            this.methodArgsType = methodArgsType;
            this.name = name;
        }

        public String field()
        {
            return String.format("%sprivate final %s %s;\n\n", INDENT, type, name);
        }

        public String getter()
        {
            return String.format("%spublic final %s %s() { return %3$s; }\n\n", INDENT, type, name);
        }

        public String declaration()
        {
            return String.format("final %s %s", type, name);
        }

        public String methodArgsDeclaration()
        {
            return String.format("final %s %s", methodArgsType, name);
        }
    }

    public static String constructor(final String name, final Var... parameters)
    {

        final String binding = Stream.of(parameters)
            .map(var -> String.format("%1$s%1$s this.%2$s = %2$s;", INDENT, var.name))
            .collect(joining("\n"));

        return String.format("%s%s(%s)\n%1$s{\n%s\n%1$s}\n\n", INDENT, name, paramDeclaration(parameters), binding);
    }

    public static String paramDeclaration(final Var[] parameters)
    {
        return Stream.of(parameters)
            .map(Var::declaration)
            .collect(joining(", "));
    }

    public static String importFor(final Class<?> cls)
    {
        return String.format("import %s;\n", cls.getCanonicalName());
    }

    public static String importFor(final String className)
    {
        return String.format("import %s;\n", className);
    }


    public static String importStaticFor(final Class<?> cls)
    {
        return String.format("import static %s.*;\n", cls.getCanonicalName());
    }

    public static String importStaticFor(final Class<?> cls, final String name)
    {
        Verify.notNull(name, "name");
        return String.format("import static %s.%s;\n", cls.getCanonicalName(), name);
    }

    public static String optionalStaticInit(final String containing)
    {
        return containing.isEmpty() ? "\n" :
            "    static\n" +
            "    {\n" +
            containing +
            "    }\n\n";
    }
}
