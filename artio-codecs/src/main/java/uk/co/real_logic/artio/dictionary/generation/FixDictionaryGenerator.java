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

import org.agrona.LangUtil;
import org.agrona.generation.OutputManager;
import uk.co.real_logic.artio.builder.*;
import uk.co.real_logic.artio.decoder.*;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.dictionary.ir.Dictionary;

import java.io.IOException;

import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.importFor;

public class FixDictionaryGenerator
{
    private static final String TEMPLATE =
        "public class FixDictionaryImpl implements FixDictionary\n" +
        "{\n" +
        "    public String beginString()\n" +
        "    {\n" +
        "        return \"%1$s\";\n" +
        "    }\n" +
        "\n" +
        "    public AbstractLogonEncoder makeLogonEncoder()\n" +
        "    {\n" +
        "        return new LogonEncoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractResendRequestEncoder makeResendRequestEncoder()\n" +
        "    {\n" +
        "        return new ResendRequestEncoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractLogoutEncoder makeLogoutEncoder()\n" +
        "    {\n" +
        "        return new LogoutEncoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractHeartbeatEncoder makeHeartbeatEncoder()\n" +
        "    {\n" +
        "        return new HeartbeatEncoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractRejectEncoder makeRejectEncoder()\n" +
        "    {\n" +
        "        return new RejectEncoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractTestRequestEncoder makeTestRequestEncoder()\n" +
        "    {\n" +
        "        return new TestRequestEncoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractSequenceResetEncoder makeSequenceResetEncoder()\n" +
        "    {\n" +
        "        return new SequenceResetEncoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractLogonDecoder makeLogonDecoder()\n" +
        "    {\n" +
        "        return new LogonDecoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractLogoutDecoder makeLogoutDecoder()\n" +
        "    {\n" +
        "        return new LogoutDecoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractRejectDecoder makeRejectDecoder()\n" +
        "    {\n" +
        "        return new RejectDecoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractTestRequestDecoder makeTestRequestDecoder()\n" +
        "    {\n" +
        "        return new TestRequestDecoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractSequenceResetDecoder makeSequenceResetDecoder()\n" +
        "    {\n" +
        "        return new SequenceResetDecoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractHeartbeatDecoder makeHeartbeatDecoder()\n" +
        "    {\n" +
        "        return new HeartbeatDecoder();\n" +
        "    }\n" +
        "\n" +
        "    public AbstractResendRequestDecoder makeResendRequestDecoder()\n" +
        "    {\n" +
        "        return new ResendRequestDecoder();\n" +
        "    }\n" +
        "\n" +
        "    public SessionHeaderDecoder makeHeaderDecoder()\n" +
        "    {\n" +
        "        return new HeaderDecoder();\n" +
        "    }\n" +
        "\n";

    private final Dictionary dictionary;
    private final OutputManager outputManager;
    private final String encoderPackage;
    private final String decoderPackage;
    private final String parentPackage;

    public FixDictionaryGenerator(
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
        outputManager.withOutput("FixDictionaryImpl", (out) ->
        {
            try
            {
                out.append(fileHeader(parentPackage));

                out.append(importFor(FixDictionary.class));

                out.append(importFor(AbstractLogonEncoder.class));
                out.append(importFor(encoderPackage + ".LogonEncoder"));
                out.append(importFor(AbstractResendRequestEncoder.class));
                out.append(importFor(encoderPackage + ".ResendRequestEncoder"));
                out.append(importFor(AbstractLogoutEncoder.class));
                out.append(importFor(encoderPackage + ".LogoutEncoder"));
                out.append(importFor(AbstractHeartbeatEncoder.class));
                out.append(importFor(encoderPackage + ".HeartbeatEncoder"));
                out.append(importFor(AbstractRejectEncoder.class));
                out.append(importFor(encoderPackage + ".RejectEncoder"));
                out.append(importFor(AbstractTestRequestEncoder.class));
                out.append(importFor(encoderPackage + ".TestRequestEncoder"));
                out.append(importFor(AbstractSequenceResetEncoder.class));
                out.append(importFor(encoderPackage + ".SequenceResetEncoder"));

                out.append(importFor(AbstractLogonDecoder.class));
                out.append(importFor(decoderPackage + ".LogonDecoder"));
                out.append(importFor(AbstractLogoutDecoder.class));
                out.append(importFor(decoderPackage + ".LogoutDecoder"));
                out.append(importFor(AbstractRejectDecoder.class));
                out.append(importFor(decoderPackage + ".RejectDecoder"));
                out.append(importFor(AbstractTestRequestDecoder.class));
                out.append(importFor(decoderPackage + ".TestRequestDecoder"));
                out.append(importFor(AbstractSequenceResetDecoder.class));
                out.append(importFor(decoderPackage + ".SequenceResetDecoder"));
                out.append(importFor(AbstractHeartbeatDecoder.class));
                out.append(importFor(decoderPackage + ".HeartbeatDecoder"));
                out.append(importFor(SessionHeaderDecoder.class));
                out.append(importFor(decoderPackage + ".ResendRequestDecoder"));
                out.append(importFor(AbstractResendRequestDecoder.class));
                out.append(importFor(decoderPackage + ".HeaderDecoder"));

                out.append(String.format(TEMPLATE, dictionary.beginString()));
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
}
