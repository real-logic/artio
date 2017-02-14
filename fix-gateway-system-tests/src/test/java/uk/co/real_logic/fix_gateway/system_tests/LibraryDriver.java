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

import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
import uk.co.real_logic.fix_gateway.library.FixLibrary;

import java.util.List;

import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.LIBRARY_LIMIT;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.libraries;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.libraryInfoById;

class LibraryDriver implements AutoCloseable
{
    private final FakeOtfAcceptor otfAcceptor = new FakeOtfAcceptor();
    private final FakeHandler handler = new FakeHandler(otfAcceptor);
    private final FixLibrary library;

    LibraryDriver()
    {
        library = SystemTestUtil.newAcceptingLibrary(handler);
    }

    long awaitSessionId()
    {
        return handler.awaitSessionId(this::poll);
    }

    public void close()
    {
        library.close();
    }

    void becomeOnlyLibraryConnectedTo(final FixEngine engine)
    {
        final int ourLibraryId = library.libraryId();
        assertEventuallyTrue(
            () -> ourLibraryId + " has failed to become the only library: " + libraries(engine),
            () ->
            {
                poll();

                final List<LibraryInfo> libraries = libraries(engine);
                return libraries.size() == 2 &&
                       libraryInfoById(libraries, ourLibraryId).isPresent();
            },
            5000,
            () -> {});
    }

    private int poll()
    {
        return library.poll(LIBRARY_LIMIT);
    }
}
