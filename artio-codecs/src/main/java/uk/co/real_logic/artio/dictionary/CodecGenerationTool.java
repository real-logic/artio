/*
 * Copyright 2015-2025 Real Logic Limited., Monotonic Ltd.
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

import uk.co.real_logic.artio.dictionary.generation.CodecConfiguration;
import uk.co.real_logic.artio.dictionary.generation.CodecGenerator;

public final class CodecGenerationTool
{
    public static void main(final String[] args)
    {
        if (args.length < 2)
        {
            printUsageAndExit();
        }

        final String outputPath = args[0];
        final String files = args[1];
        final String[] fileNames = files.split(";");

        try
        {
            final CodecConfiguration config = new CodecConfiguration()
                .outputPath(outputPath)
                .fileNames(fileNames);
            CodecGenerator.generate(config);
        }
        catch (final Throwable e)
        {
            e.printStackTrace();
            printUsageAndExit();
        }
    }

    private static void printUsageAndExit()
    {
        System.err.println("Usage: CodecGenerationTool </path/to/output-directory> " +
            "<[/path/to/fixt-xml/dictionary;]/path/to/xml/dictionary>");
        System.exit(-1);
    }
}
