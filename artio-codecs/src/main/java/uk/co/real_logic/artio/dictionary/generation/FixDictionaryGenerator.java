/*
 * Copyright 2019 Monotonic Ltd.
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

import java.io.IOException;
import java.io.Writer;
import java.util.Set;
import java.util.stream.Collectors;

import org.agrona.LangUtil;
import org.agrona.generation.OutputManager;


import uk.co.real_logic.artio.builder.*;
import uk.co.real_logic.artio.decoder.AbstractHeartbeatDecoder;
import uk.co.real_logic.artio.decoder.AbstractLogonDecoder;
import uk.co.real_logic.artio.decoder.AbstractLogoutDecoder;
import uk.co.real_logic.artio.decoder.AbstractRejectDecoder;
import uk.co.real_logic.artio.decoder.AbstractResendRequestDecoder;
import uk.co.real_logic.artio.decoder.AbstractSequenceResetDecoder;
import uk.co.real_logic.artio.decoder.AbstractTestRequestDecoder;
import uk.co.real_logic.artio.decoder.AbstractUserRequestDecoder;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.dictionary.Generated;
import uk.co.real_logic.artio.dictionary.ir.Aggregate;
import uk.co.real_logic.artio.dictionary.ir.Dictionary;

import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.GENERATED_ANNOTATION;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.importFor;

class FixDictionaryGenerator
{
    private static final String TEMPLATE =
        "public class FixDictionaryImpl implements FixDictionary\n" +
        "{\n" +
        "    public String beginString()\n" +
        "    {\n" +
        "        return \"%1$s\";\n" +
        "    }\n" +
        "\n" +
        "    public SessionHeaderDecoder makeHeaderDecoder()\n" +
        "    {\n" +
        "        return new HeaderDecoder();\n" +
        "    }\n" +
        "\n" +
        "    public SessionHeaderEncoder makeHeaderEncoder()\n" +
        "    {\n" +
        "        return new HeaderEncoder();\n" +
        "    }\n" +
        "\n";

    private static final String MAKE_TEMPLATE = "" +
        "    public Abstract%1$s make%1$s()\n" +
        "    {\n" +
        "        return %2$s;\n" +
        "    }\n" +
        "\n";

    private static final String NULL = "null";
    private final Dictionary dictionary;
    private final OutputManager outputManager;
    private final String encoderPackage;
    private final String decoderPackage;
    private final String parentPackage;

    FixDictionaryGenerator(
        final Dictionary dictionary,
        final OutputManager outputManager,
        final String encoderPackage,
        final String decoderPackage,
        final String parentPackage)
    {
        this.dictionary = dictionary;
        this.outputManager = outputManager;
        this.encoderPackage = encoderPackage;
        this.decoderPackage = decoderPackage;
        this.parentPackage = parentPackage;
    }

    public void generate()
    {
        if (dictionary.shared())
        {
            return;
        }

        final Set<String> allMessageNames = dictionary.messages()
            .stream().map(Aggregate::name).collect(Collectors.toSet());

        outputManager.withOutput("FixDictionaryImpl", (out) ->
        {
            try
            {
                final StringBuilder sb = new StringBuilder(String.format(TEMPLATE, dictionary.beginString()));
                out.append(fileHeader(parentPackage));

                out.append(importFor(FixDictionary.class));

                out.append(importFor(AbstractLogonEncoder.class));
                addEncoderImport(out, encoderPackage, "Logon", allMessageNames, sb);
                out.append(importFor(AbstractResendRequestEncoder.class));
                addEncoderImport(out, encoderPackage, "ResendRequest", allMessageNames, sb);
                out.append(importFor(AbstractLogoutEncoder.class));
                addEncoderImport(out, encoderPackage, "Logout", allMessageNames, sb);
                out.append(importFor(AbstractHeartbeatEncoder.class));
                addEncoderImport(out, encoderPackage, "Heartbeat", allMessageNames, sb);
                out.append(importFor(AbstractRejectEncoder.class));
                addEncoderImport(out, encoderPackage, "Reject", allMessageNames, sb);
                out.append(importFor(AbstractTestRequestEncoder.class));
                addEncoderImport(out, encoderPackage, "TestRequest", allMessageNames, sb);
                out.append(importFor(AbstractSequenceResetEncoder.class));
                addEncoderImport(out, encoderPackage, "SequenceReset", allMessageNames, sb);
                out.append(importFor(AbstractBusinessMessageRejectEncoder.class));
                addEncoderImport(out, encoderPackage, "BusinessMessageReject", allMessageNames, sb);

                out.append(importFor(SessionHeaderEncoder.class));
                out.append(importFor(encoderPackage + ".HeaderEncoder"));

                out.append(importFor(AbstractLogonDecoder.class));
                addDecoderImport(out, decoderPackage, "Logon", allMessageNames, sb);
                out.append(importFor(AbstractLogoutDecoder.class));
                addDecoderImport(out, decoderPackage, "Logout", allMessageNames, sb);
                out.append(importFor(AbstractRejectDecoder.class));
                addDecoderImport(out, decoderPackage, "Reject", allMessageNames, sb);
                out.append(importFor(AbstractTestRequestDecoder.class));
                addDecoderImport(out, decoderPackage, "TestRequest", allMessageNames, sb);
                out.append(importFor(AbstractSequenceResetDecoder.class));
                addDecoderImport(out, decoderPackage, "SequenceReset", allMessageNames, sb);
                out.append(importFor(AbstractHeartbeatDecoder.class));
                addDecoderImport(out, decoderPackage, "Heartbeat", allMessageNames, sb);
                out.append(importFor(AbstractResendRequestDecoder.class));
                addDecoderImport(out, decoderPackage, "ResendRequest", allMessageNames, sb);

                out.append(importFor(AbstractUserRequestDecoder.class));
                addDecoderImport(out, decoderPackage, "UserRequest", allMessageNames, sb);

                out.append(importFor(SessionHeaderDecoder.class));
                out.append(importFor(decoderPackage + ".HeaderDecoder"));
                out.append(importFor(Generated.class));

                out.append("\n" + GENERATED_ANNOTATION);
                out.append(sb.toString());
            }
            catch (final IOException e)
            {
                LangUtil.rethrowUnchecked(e);
            }
            finally
            {
                out.append("}\n");
            }
        });
    }

    private static void addEncoderImport(
        final Writer out,
        final String encoderPackage,
        final String messageName,
        final Set<String> allMessageNames,
        final StringBuilder sb) throws IOException
    {
        addImport(out, encoderPackage, messageName, allMessageNames, sb, "Encoder");
    }

    private static void addDecoderImport(
        final Writer out,
        final String decoderPackage,
        final String messageName,
        final Set<String> allMessageNames, final StringBuilder sb) throws IOException
    {
        addImport(out, decoderPackage, messageName, allMessageNames, sb, "Decoder");
    }

    private static void addImport(
        final Writer out,
        final String classPackage,
        final String messageName,
        final Set<String> allMessageNames,
        final StringBuilder sb,
        final String encoderOrDecoder) throws IOException
    {
        final String encoderClass = messageName + encoderOrDecoder;
        final String constructor;
        if (allMessageNames.contains(messageName))
        {
            constructor = String.format("new %s()", encoderClass);
            out.append(importFor(classPackage + "." + encoderClass));
        }
        else
        {
            constructor = NULL;
        }

        sb.append(String.format(MAKE_TEMPLATE, encoderClass, constructor));
    }
}
