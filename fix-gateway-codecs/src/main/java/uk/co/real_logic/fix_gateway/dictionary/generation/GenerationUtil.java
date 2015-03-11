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
package uk.co.real_logic.fix_gateway.dictionary.generation;

import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public final class GenerationUtil
{
    private GenerationUtil()
    {
    }

    public static final String PARENT_PACKAGE = "uk.co.real_logic.fix_gateway";
    public static final String ENCODER_PACKAGE = PARENT_PACKAGE + ".builder";
    public static final String DECODER_PACKAGE = PARENT_PACKAGE + ".decoder";

    public static final String INDENT = "    ";

    public static String fileHeader(final String packageName)
    {
        return String.format(
                "/* Generated Fix Gateway message codec */\n" +
                "package %s;\n\n",
                packageName
        );
    }

    public static class Var
    {
        private final String type;
        private final String name;

        public Var(final String type, final String name)
        {
            this.type = type;
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
    }

    public static String constructor(final String name, final Var ... parameters)
    {

        final String binding = Stream.of(parameters)
                                     .map(var -> String.format("%1$s%1$s this.%2$s = %2$s;", INDENT, var.name))
                                     .collect(joining("\n"));

        return String.format("%s%s(%s)\n%1$s{\n%s\n%1$s}\n\n", INDENT, name, paramDeclaration(parameters), binding);
    }

    public static String method(final String name, final String returnType, final Var ... parameters)
    {
        return String.format("%spublic static %s %s(%s)\n%1$s{\n",
                             INDENT, returnType, name, paramDeclaration(parameters));
    }

    private static String paramDeclaration(final Var[] parameters)
    {
        return Stream.of(parameters)
                .map(Var::declaration)
                .collect(joining(", "));
    }

    public static String importFor(Class<?> cls)
    {
        return String.format("import %s;\n", cls.getCanonicalName());
    }

    public static String importStaticFor(Class<?> cls)
    {
        return String.format("import static %s.*;\n", cls.getCanonicalName());
    }

}
