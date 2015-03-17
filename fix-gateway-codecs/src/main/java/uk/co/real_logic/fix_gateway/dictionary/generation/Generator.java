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

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.generation.OutputManager;
import uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants;
import uk.co.real_logic.fix_gateway.dictionary.ir.Aggregate;
import uk.co.real_logic.fix_gateway.dictionary.ir.DataDictionary;
import uk.co.real_logic.fix_gateway.dictionary.ir.Entry;
import uk.co.real_logic.fix_gateway.fields.DecimalFloat;
import uk.co.real_logic.fix_gateway.fields.LocalMktDateEncoder;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.importFor;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.importStaticFor;

public abstract class Generator
{

    protected static final String MSG_TYPE = "MsgType";

    protected String commonCompoundImports(final String form)
    {
        return String.format(
            "    private Header%s header = new Header%1$s();\n\n" +
            "    public Header%1$s header() {\n" +
            "        return header;\n" +
            "    }\n\n" +

            "    private Trailer%1$s trailer = new Trailer%1$s();\n\n" +
            "    public Trailer%1$s trailer() {\n" +
            "        return trailer;\n" +
            "    }\n\n",
            form);
    }

    private static final String COMMON_COMPOUND_IMPORTS =
            "import %1$s.Header%3$s;\n" +
            "import %1$s.Trailer%3$s;\n";

    protected final DataDictionary dictionary;
    protected final String builderPackage;
    protected final OutputManager outputManager;

    protected Generator(final DataDictionary dictionary, final String builderPackage, final OutputManager outputManager)
    {
        this.dictionary = dictionary;
        this.builderPackage = builderPackage;
        this.outputManager = outputManager;
    }

    public void generate()
    {
        generateAggregate(dictionary.header(), AggregateType.HEADER);
        generateAggregate(dictionary.trailer(), AggregateType.TRAILER);

        dictionary.messages()
                .forEach(msg -> generateAggregate(msg, AggregateType.MESSAGE));
    }

    protected abstract void generateAggregate(final Aggregate aggregate, final AggregateType type);

    protected String generateClassDeclaration(
        final String className,
        final boolean hasCommonCompounds,
        final Class<?> parent)
    {
        return String.format(
            importFor(MutableDirectBuffer.class) +
            importStaticFor(CodecUtil.class) +
            importStaticFor(StandardFixConstants.class) +
            importFor(parent) +
            (hasCommonCompounds ? COMMON_COMPOUND_IMPORTS : "") +
            importFor(DecimalFloat.class) +
            importFor(MutableAsciiFlyweight.class) +
            importFor(AsciiFlyweight.class) +
            importFor(LocalMktDateEncoder.class) +
            importFor(UtcTimestampEncoder.class) +
            importFor(StandardCharsets.class) +
            "\n" +
            "public class %2$s implements %3$s\n" +
            "{\n\n",
            builderPackage,
            className,
            parent.getSimpleName());
    }

    protected String generateResetMethod(List<Entry> entries)
    {
        return "    public void reset() {\n" +
               "    }\n\n";
    }

    protected String optionalField(final Entry entry)
    {
        return entry.required() ? "" : String.format("    private boolean has%s;\n\n", entry.name());
    }
}
