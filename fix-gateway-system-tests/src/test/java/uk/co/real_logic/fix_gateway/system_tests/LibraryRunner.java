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
import uk.co.real_logic.fix_gateway.library.Reply;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.session.Session;

public class LibraryRunner
{
    private final FixLibrary library;

    public LibraryRunner(final FixLibrary library)
    {
        this.library = library;
    }

    public Session initiate(final SessionConfiguration configuration)
    {
        final Reply<Session> reply = library.initiate2(configuration);

        while (reply.isExecuting())
        {
            library.poll(1);
        }

        return reply.resultIfPresent();
    }
}
