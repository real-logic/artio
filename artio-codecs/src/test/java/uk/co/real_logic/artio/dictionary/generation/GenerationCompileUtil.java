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

import org.agrona.generation.CharSequenceJavaFileObject;
import org.agrona.generation.ClassFileManager;
import org.agrona.generation.CompilerUtil;

import javax.tools.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

class GenerationCompileUtil
{

    static Class<?> compileCleanInMemory(
        final String className, final Map<String, CharSequence> sources)
        throws ClassNotFoundException
    {
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (null == compiler)
        {
            throw new IllegalStateException("JDK required to run tests. JRE is not sufficient.");
        }

        final JavaFileManager fileManager = new ClassFileManager<>(
            compiler.getStandardFileManager(null, null, null));
        final DiagnosticCollector<JavaFileObject> diagnosticCollector = new DiagnosticCollector<>();
        final Iterable<String> options = Arrays.asList("-proc:none", "-Xlint:unchecked");
        final JavaCompiler.CompilationTask task = compiler.getTask(
            null, fileManager, diagnosticCollector, options, null, wrap(sources));

        final Class<?> clazz = CompilerUtil.compileAndLoad(className, diagnosticCollector, fileManager, task);
        if (clazz != null)
        {
            final List<Diagnostic<? extends JavaFileObject>> diagnostics = diagnosticCollector.getDiagnostics();
            assertThat(diagnostics.toString(), diagnostics, hasSize(0));
        }
        return clazz;
    }

    private static Collection<CharSequenceJavaFileObject> wrap(final Map<String, CharSequence> sources)
    {
        return sources
            .entrySet()
            .stream()
            .map((e) -> new CharSequenceJavaFileObject(e.getKey(), e.getValue()))
            .collect(toList());
    }
}
