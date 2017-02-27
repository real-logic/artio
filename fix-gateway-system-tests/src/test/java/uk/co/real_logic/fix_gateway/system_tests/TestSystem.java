/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.system_tests;

import uk.co.real_logic.fix_gateway.library.FixLibrary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertThat;
import static uk.co.real_logic.fix_gateway.FixMatchers.isConnected;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.LIBRARY_LIMIT;

public class TestSystem
{
    private final List<FixLibrary> libraries;

    public TestSystem(final FixLibrary ... libraries)
    {
        this.libraries = new ArrayList<>();
        Collections.addAll(this.libraries, libraries);
    }

    public void poll()
    {
        libraries.forEach(library -> library.poll(LIBRARY_LIMIT));
    }

    public void assertConnected()
    {
        libraries.forEach(library -> assertThat(library, isConnected()));
    }

    public void close(final FixLibrary library)
    {
        library.close();
        libraries.remove(library);
    }

    public FixLibrary add(final FixLibrary library)
    {
        libraries.add(library);
        return library;
    }
}
